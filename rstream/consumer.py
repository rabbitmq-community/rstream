from __future__ import annotations

import asyncio
import inspect
import logging
import random
import ssl
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

from . import exceptions, schema
from .amqp import AMQPMessage
from .client import Addr, Client, ClientPool
from .constants import (
    SUBSCRIPTION_PROPERTY_FILTER_PREFIX,
    SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED,
    ConsumerOffsetSpecification,
    Key,
    OffsetType,
    SlasMechanism,
)
from .schema import OffsetSpecification
from .utils import FilterConfiguration, OnClosedErrorInfo

MT = TypeVar("MT")
CB = Annotated[Callable[[MT, Any], Union[None, Awaitable[None]]], "Message callback type"]
CB_CONN = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]
logger = logging.getLogger(__name__)


@dataclass
class MessageContext:
    consumer: Consumer
    subscriber_name: str
    offset: int
    timestamp: int


@dataclass
class EventContext:
    consumer: Consumer
    subscriber_name: str
    reference: str


@dataclass
class _Subscriber:
    stream: str
    subscription_id: int
    reference: str
    client: Client
    callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]]
    decoder: Callable[[bytes], Any]
    offset_type: OffsetType
    offset: int
    filter_input: Optional[FilterConfiguration]


class Consumer:
    def __init__(
        self,
        host: str,
        port: int = 5552,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        vhost: str = "/",
        username: str,
        password: str,
        frame_max: int = 1 * 1024 * 1024,
        heartbeat: int = 60,
        load_balancer_mode: bool = False,
        max_retries: int = 20,
        max_subscribers_by_connection: int = 256,
        on_close_handler: Optional[CB_CONN[OnClosedErrorInfo]] = None,
        connection_name: str = None,
        sasl_configuration_mechanism: SlasMechanism = SlasMechanism.MechanismPlain,
    ):
        self._pool = ClientPool(
            host,
            port,
            ssl_context=ssl_context,
            vhost=vhost,
            username=username,
            password=password,
            frame_max=frame_max,
            heartbeat=heartbeat,
            load_balancer_mode=load_balancer_mode,
            max_retries=max_retries,
            sasl_configuration_mechanism=sasl_configuration_mechanism,
        )

        self._default_client: Optional[Client] = None
        self._clients: dict[str, Client] = {}
        self._subscribers: dict[str, _Subscriber] = {}
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._on_close_handler = on_close_handler
        self._connection_name = connection_name
        self._sasl_configuration_mechanism = sasl_configuration_mechanism
        if self._connection_name is None:
            self._connection_name = "rstream-consumer"
        self._max_subscribers_by_connection = max_subscribers_by_connection

    @property
    async def default_client(self) -> Client:
        if self._default_client is None:
            self._default_client = await self._create_locator_connection()
        return self._default_client

    async def __aenter__(self) -> Consumer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get(
            connection_closed_handler=self._on_close_handler,
            connection_name=self._connection_name,
            sasl_configuration_mechanism=self._sasl_configuration_mechanism,
            max_clients_by_connections=self._max_subscribers_by_connection,
        )

    def stop(self) -> None:
        self._stop_event.set()

    async def close(self) -> None:
        logger.debug("Closing Consumer and clean up")
        self.stop()

        logger.debug("close(): Unsubscribe subscribers")
        for subscriber in list(self._subscribers.values()):
            if subscriber.client.is_connection_alive():
                await self.unsubscribe(subscriber.reference)

        logger.debug("close(): Cleaning up structs")
        self._subscribers.clear()

        await self._pool.close()
        self._clients.clear()
        self._default_client = None

    async def run(self) -> None:
        await self._stop_event.wait()

    async def _get_or_create_client(self, stream: str) -> Client:
        logger.debug("_get_or_create_client(): Get or create new client/connection")
        if stream not in self._clients:
            if self._default_client is None:
                logger.debug("_get_or_create_client(): Creating locator connection")
                self._default_client = await self._pool.get(
                    connection_closed_handler=self._on_close_handler,
                    connection_name=self._connection_name,
                    max_clients_by_connections=self._max_subscribers_by_connection,
                )

            leader, replicas = await (await self.default_client).query_leader_and_replicas(stream)
            broker = random.choice(replicas) if replicas else leader
            logger.debug("_get_or_create_client(): Getting/Creating connection")
            self._clients[stream] = await self._pool.get(
                addr=Addr(broker.host, broker.port),
                connection_closed_handler=self._on_close_handler,
                connection_name=self._connection_name,
                stream=stream,
                max_clients_by_connections=self._max_subscribers_by_connection,
            )

            await self._close_locator_connection()

        return self._clients[stream]

    async def _create_subscriber(
        self,
        stream: str,
        subscriber_name: Optional[str],
        callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]],
        decoder: Optional[Callable[[bytes], Any]],
        offset_type: OffsetType,
        offset: Optional[int],
        filter_input: Optional[FilterConfiguration],
    ) -> _Subscriber:
        logger.debug("_create_subscriber(): Create subscriber")
        client = await self._get_or_create_client(stream)

        # We can have multiple subscribers sharing same connection, so their ids must be distinct
        # subscription_id = len([s for s in self._subscribers.values() if s.client is client]) + 1
        subscription_id = await client.get_available_id()
        reference = subscriber_name or f"{stream}_subscriber_{subscription_id}"
        decoder = decoder or (lambda x: x)

        subscriber = self._subscribers[reference] = _Subscriber(
            stream=stream,
            subscription_id=subscription_id,
            client=client,
            reference=reference,
            callback=callback,
            decoder=decoder,
            offset_type=offset_type,
            offset=offset or 0,
            filter_input=filter_input,
        )
        return subscriber

    async def subscribe(
        self,
        stream: str,
        callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]],
        *,
        decoder: Optional[Callable[[bytes], MT]] = None,
        offset_specification: Optional[ConsumerOffsetSpecification] = None,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscriber_name: Optional[str] = None,
        consumer_update_listener: Optional[
            Callable[[bool, EventContext], Awaitable[OffsetSpecification]]
        ] = None,
        filter_input: Optional[FilterConfiguration] = None,
    ) -> str:
        logger.debug("Consumer subscribe()")
        if offset_specification is None:
            offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)

        async with self._lock:
            logger.debug("subscribe(): Create subscriber")
            subscriber = await self._create_subscriber(
                stream=stream,
                subscriber_name=subscriber_name,
                callback=callback,
                decoder=decoder,
                offset_type=offset_specification.offset_type,
                offset=offset_specification.offset,
                filter_input=filter_input,
            )

            await subscriber.client.run_queue_listener_task(
                subscriber_name=subscriber.reference,
                handler=partial(self._on_deliver, subscriber=subscriber, filter_value=filter_input),
            )

        logger.debug("subscribe(): Adding handlers")
        subscriber.client.add_handler(
            schema.Deliver,
            partial(self._on_deliver, subscriber=subscriber, filter_value=filter_input),
            name=subscriber.reference,
        )

        subscriber.client.add_handler(
            schema.MetadataUpdate,
            partial(self._on_metadata_update),
            name=subscriber.reference,
        )

        # to handle single-active-consumer
        if properties is not None:
            if "single-active-consumer" in properties:
                logger.debug("subscribe(): Enabling SAC")
                subscriber.client.add_handler(
                    schema.ConsumerUpdateResponse,
                    partial(
                        self._on_consumer_update_query_response,
                        subscriber=subscriber,
                        reference=properties["name"],
                        consumer_update_listener=consumer_update_listener,
                    ),
                    name=subscriber.reference,
                )

        if filter_input is not None:
            logger.debug("subscribe(): Filtering scenario enabled")
            await self._check_if_filtering_is_supported()
            values_to_filter = filter_input.values()
            if len(values_to_filter) <= 0:
                raise ValueError("you need to specify at least one filter value")

            if properties is None:
                properties = defaultdict(str)
            for i, filter_value in enumerate(values_to_filter):
                key = SUBSCRIPTION_PROPERTY_FILTER_PREFIX + str(i)
                properties[key] = filter_value
            if filter_input.match_unfiltered():
                properties[SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED] = "true"
            else:
                properties[SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED] = "false"

        logger.debug("subscribe(): Subscribing")
        await subscriber.client.subscribe(
            stream=stream,
            subscription_id=subscriber.subscription_id,
            offset_spec=schema.OffsetSpec.from_params(
                offset_specification.offset_type, offset_specification.offset
            ),
            initial_credit=initial_credit,
            properties=properties,
        )

        return subscriber.reference

    async def unsubscribe(self, subscriber_name: str) -> None:
        logger.debug("unsubscribe(): UnSubscribing and removing handlers")
        subscriber = self._subscribers[subscriber_name]

        await subscriber.client.stop_queue_listener_task(subscriber_name=subscriber_name)
        subscriber.client.remove_handler(
            schema.Deliver,
            name=subscriber.reference,
        )
        subscriber.client.remove_handler(
            schema.MetadataUpdate,
            name=subscriber.reference,
        )
        try:
            await asyncio.wait_for(subscriber.client.unsubscribe(subscriber.subscription_id), 5)
        except asyncio.TimeoutError:
            logger.warning("timeout when closing consumer and deleting publisher")
        except BaseException as exc:
            logger.warning("exception in unsubscribe of Consumer: %s", exc)

        stream = subscriber.stream

        if stream in self._clients:
            await self._clients[stream].remove_stream(stream)
            await self._clients[stream].free_available_id(subscriber.subscription_id)

        del self._subscribers[subscriber_name]

    async def query_offset(self, stream: str, subscriber_name: str) -> int:
        if subscriber_name == "":
            raise ValueError("subscriber_name must not be an empty string")

        async with self._lock:
            offset = await (await self.default_client).query_offset(
                stream,
                subscriber_name,
            )
            await self._close_locator_connection()

        return offset

    async def store_offset(self, stream: str, subscriber_name: str, offset: int) -> None:
        async with self._lock:
            await (await self.default_client).store_offset(
                stream=stream,
                reference=subscriber_name,
                offset=offset,
            )
            await self._close_locator_connection()

    @staticmethod
    def _filter_messages(
        frame: schema.Deliver, subscriber: _Subscriber, filter_value: Optional[FilterConfiguration] = None
    ) -> Iterator[tuple[int, bytes]]:
        min_deliverable_offset = -1
        is_filtered = True
        if subscriber.offset_type is OffsetType.OFFSET:
            min_deliverable_offset = subscriber.offset

        offset = frame.chunk_first_offset - 1

        for message in frame.get_messages():
            offset += 1
            if offset < min_deliverable_offset:
                continue
            if filter_value is not None:
                filter_predicate = filter_value.post_filler()
                if filter_predicate is not None:
                    is_filtered = filter_predicate(subscriber.decoder(message))

            if is_filtered:
                yield (offset, message)

        subscriber.offset = frame.chunk_first_offset + frame.num_entries

    async def _on_deliver(
        self, frame: schema.Deliver, subscriber: _Subscriber, filter_value: Optional[FilterConfiguration]
    ) -> None:
        if frame.subscription_id != subscriber.subscription_id:
            return

        await subscriber.client.credit(subscriber.subscription_id, 1)

        for offset, message in self._filter_messages(frame, subscriber, filter_value):
            message_context = MessageContext(self, subscriber.reference, offset, frame.timestamp)

            maybe_coro = subscriber.callback(subscriber.decoder(message), message_context)
            if maybe_coro is not None:
                await maybe_coro

    async def _on_metadata_update(self, frame: schema.MetadataUpdate) -> None:
        logger.debug("_on_metadata_update: On metadata update event triggered on producer")
        if frame.metadata_info.stream not in self._clients:
            return
        await self._maybe_clean_up_during_lost_connection(frame.metadata_info.stream)
        if self._on_close_handler is not None:
            logger.debug("_on_metadata_update: on_close_handler provided calling")
            metadata_update_info = OnClosedErrorInfo("MetaData Update", [frame.metadata_info.stream])
            result = self._on_close_handler(metadata_update_info)
            if result is not None and inspect.isawaitable(result):
                await result

    async def _on_consumer_update_query_response(
        self,
        frame: schema.ConsumerUpdateResponse,
        subscriber: _Subscriber,
        reference: str,
        consumer_update_listener: Optional[
            Callable[[bool, EventContext], Awaitable[OffsetSpecification]]
        ] = None,
    ) -> None:
        if frame.subscription_id != subscriber.subscription_id:
            return

        # event the consumer is not active, we need to send a ConsumerUpdateResponse
        # by protocol definition. the offsetType can't be null so we use OffsetTypeNext as default
        if consumer_update_listener is None:
            offset_specification = OffsetSpecification(OffsetType.NEXT, 0)
            await subscriber.client.consumer_update(frame.correlation_id, offset_specification)

        else:
            is_active = bool(frame.active)
            event_context = EventContext(self, subscriber.reference, reference)
            offset_specification = await consumer_update_listener(is_active, event_context)
            subscriber.offset_type = OffsetType(offset_specification.offset_type)
            subscriber.offset = offset_specification.offset
            await subscriber.client.consumer_update(frame.correlation_id, offset_specification)

    async def create_stream(
        self,
        stream: str,
        arguments: Optional[dict[str, Any]] = None,
        exists_ok: bool = False,
    ) -> None:
        async with self._lock:
            try:
                await (await self.default_client).create_stream(stream, arguments)

            except exceptions.StreamAlreadyExists:
                if not exists_ok:
                    raise
            finally:
                await self._close_locator_connection()

    async def clean_up_subscribers(self, stream: str):
        for subscriber in list(self._subscribers.values()):
            if subscriber.stream == stream:
                del self._subscribers[subscriber.reference]

    async def delete_stream(self, stream: str, missing_ok: bool = False) -> None:
        await self.clean_up_subscribers(stream)

        async with self._lock:
            try:
                await (await self.default_client).delete_stream(stream)

            except exceptions.StreamDoesNotExist:
                if not missing_ok:
                    raise
            finally:
                await self._close_locator_connection()

    async def stream_exists(self, stream: str, on_close_event: bool = False) -> bool:
        async with self._lock:
            if on_close_event:
                self._default_client = None
            stream_exists = await (await self.default_client).stream_exists(stream)
            await self._close_locator_connection()

        return stream_exists

    async def stream(self, subscriber_name) -> str:
        if subscriber_name not in self._subscribers:
            return ""
        return self._subscribers[subscriber_name].stream

    def get_stream(self, subscriber_name) -> str:
        if subscriber_name not in self._subscribers:
            return ""
        return self._subscribers[subscriber_name].stream

    async def reconnect_stream(self, stream: str, offset: Optional[int] = None) -> None:
        logging.debug("reconnect_stream")
        curr_subscriber = None
        curr_subscriber_id = None
        for subscriber_id in self._subscribers:
            if stream == self._subscribers[subscriber_id].stream:
                curr_subscriber = self._subscribers[subscriber_id]
                curr_subscriber_id = subscriber_id
        if curr_subscriber_id is not None:
            del self._subscribers[curr_subscriber_id]

        if stream in self._clients:
            if curr_subscriber is not None:
                await self._clients[stream].free_available_id(curr_subscriber.subscription_id)
            await self._clients[stream].close()
            del self._clients[stream]

        if self._default_client is not None:
            if self._default_client.is_connection_alive() is False:
                await self._default_client.close()
                self._default_client = None

        if offset is None:
            if curr_subscriber is not None:
                offset = curr_subscriber.offset

        logging.debug("reconnect_stream(): Subscribing again")
        offset_specification = ConsumerOffsetSpecification(OffsetType.OFFSET, offset)
        if curr_subscriber is not None:
            asyncio.create_task(
                self.subscribe(
                    stream=curr_subscriber.stream,
                    # subscriber_name=curr_subscriber.reference,
                    callback=curr_subscriber.callback,
                    decoder=curr_subscriber.decoder,
                    offset_specification=offset_specification,
                    filter_input=curr_subscriber.filter_input,
                )
            )

    async def _check_if_filtering_is_supported(self) -> None:
        command_version_input = schema.FrameHandlerInfo(Key.Publish.value, min_version=1, max_version=2)
        server_command_version: schema.FrameHandlerInfo = await (
            await self.default_client
        ).exchange_command_version(command_version_input)
        await self._close_locator_connection()
        if server_command_version.max_version < 2:
            filter_not_supported = (
                "Filtering is not supported by the broker "
                + "(requires RabbitMQ 3.13+ and stream_filtering feature flag activated)"
            )
            raise ValueError(filter_not_supported)

    async def _create_locator_connection(self) -> Client:
        return await self._pool.get(
            connection_closed_handler=self._on_close_handler,
            connection_name=self._connection_name,
            sasl_configuration_mechanism=self._sasl_configuration_mechanism,
        )

    async def _close_locator_connection(self):
        if await (await self.default_client).get_stream_count() == 0:
            await (await self.default_client).close()
            self._default_client = None

    async def _maybe_clean_up_during_lost_connection(self, stream: str):
        curr_subscriber = None

        for subscriber_id in self._subscribers:
            if stream == self._subscribers[subscriber_id].stream:
                curr_subscriber = self._subscribers[subscriber_id]

        if stream in self._clients:
            await self._clients[stream].remove_stream(stream)
            if curr_subscriber is not None:
                await self._clients[stream].free_available_id(curr_subscriber.subscription_id)
            if await self._clients[stream].get_stream_count() == 0:
                await self._clients[stream].close()
            del self._clients[stream]
