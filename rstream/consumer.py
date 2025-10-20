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
    Any,
    Awaitable,
    Callable,
    Iterator,
    Optional,
    Union,
)

from . import exceptions, schema, utils
from .amqp import AMQPMessage
from .client import Addr, Client, ClientPool
from .constants import (
    MAX_ITEM_ALLOWED,
    SUBSCRIPTION_PROPERTY_FILTER_PREFIX,
    SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED,
    ConsumerOffsetSpecification,
    Key,
    OffsetType,
    SlasMechanism,
)
from .exceptions import StreamAlreadySubscribed
from .recovery import (
    CB_CONN,
    MT,
    BackOffRecoveryStrategy,
    IReliableEntity,
    RecoveryStrategy,
)
from .schema import OffsetSpecification
from .utils import FilterConfiguration, OnClosedErrorInfo

logger = logging.getLogger(__name__)


@dataclass
class MessageContext:
    consumer: Consumer
    stream: str
    subscriber_id: int
    subscriber_name: Optional[str]
    offset: int
    timestamp: int


@dataclass
class EventContext:
    consumer: Consumer
    stream: str
    subscriber_id: int
    subscriber_name: Optional[str]
    reference: str


@dataclass
class _Subscriber:
    stream: str
    client: Client
    subscription_id: int
    reference: Optional[str]
    callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]]
    decoder: Callable[[bytes], Any]
    offset_type: OffsetType
    offset: int
    filter_input: Optional[FilterConfiguration]


class Consumer(IReliableEntity):
    def __init__(
        self,
        host: str,
        port: int = 5552,
        *,
        username: str,
        password: str,
        ssl_context: Optional[ssl.SSLContext] = None,
        vhost: str = "/",
        frame_max: int = 1 * 1024 * 1024,
        heartbeat: int = 60,
        load_balancer_mode: bool = False,
        max_retries: int = 20,
        max_subscribers_by_connection: int = MAX_ITEM_ALLOWED,
        on_close_handler: Optional[CB_CONN[OnClosedErrorInfo]] = None,
        connection_name: str = "",
        sasl_configuration_mechanism: SlasMechanism = SlasMechanism.MechanismPlain,
        recovery_strategy: RecoveryStrategy = BackOffRecoveryStrategy(),
    ):
        super().__init__()
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

        # validate max_subscribers_by_connection
        if max_subscribers_by_connection <= 0:
            raise ValueError("max_subscribers_by_connection must be greater than 0")

        if max_subscribers_by_connection > MAX_ITEM_ALLOWED:
            raise ValueError(f"max_subscribers_by_connection must be less than {MAX_ITEM_ALLOWED}")

        self._recovery_strategy = recovery_strategy
        self._default_client: Optional[Client] = None
        self._clients: dict[str, Client] = {}
        self._subscribers: dict[int, _Subscriber] = {}
        self._last_subscriber_id = utils.AtomicInteger(-1)
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._on_close_handler = on_close_handler
        self._connection_name = connection_name
        self._sasl_configuration_mechanism = sasl_configuration_mechanism
        if self._connection_name is None or self._connection_name == "":
            self._connection_name = "rstream-consumer"
        self._max_subscribers_by_connection = max_subscribers_by_connection

    @property
    async def default_client(self) -> Client:
        if self._default_client is None or not self._default_client.is_connection_alive():
            self._default_client = await self._create_locator_connection()
        return self._default_client

    async def __aenter__(self) -> Consumer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._default_client = await self._pool.get(
            connection_closed_handler=self._on_connection_closed,
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
                await self.unsubscribe(subscriber.subscription_id)

        logger.debug("close(): Cleaning up structs")
        self._subscribers.clear()

        await self._pool.close()
        self._clients.clear()
        self._default_client = None

    async def run(self) -> None:
        await self._stop_event.wait()

    async def _get_or_create_client(self, stream: str) -> Client:
        logger.debug(
            "[get_or_create_client] Get or create new client/connection for stream: {}".format(stream)
        )
        if stream not in self._clients:
            if self._default_client is None:
                logger.debug(
                    "[get_or_create_client] Creating locator connection for stream: {}".format(stream)
                )
                self._default_client = await self._pool.get(
                    connection_closed_handler=self._on_connection_closed,
                    connection_name=self._connection_name,
                    max_clients_by_connections=self._max_subscribers_by_connection,
                )

            leader, replicas = await (await self.default_client).query_leader_and_replicas(stream)
            broker = random.choice(replicas) if replicas else leader
            logger.debug(
                "[get_or_create_client] Getting/Creating connection for broker: {}:{}".format(
                    broker.host, broker.port
                )
            )
            self._clients[stream] = await self._pool.get(
                addr=Addr(broker.host, broker.port),
                connection_closed_handler=self._on_connection_closed,
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
        logger.debug("[create_subscriber] Create subscriber for stream : {}".format(stream))
        #  need to check if the current subscribers for this stream reached the max limit
        # We can have multiple subscribers sharing same connection, so their ids must be distinct

        # select to see if there is already a stream for this instance

        for value in self._subscribers.values():
            if value.stream == stream:
                raise StreamAlreadySubscribed("Stream  {} already subscribed".format(stream))

        client = await self._get_or_create_client(stream)
        await client.inc_available_id()
        subscription_id = await self.get_available_id()

        decoder = decoder or (lambda x: x)
        # the ID is unique per connection
        subscriber = self._subscribers[subscription_id] = _Subscriber(
            stream=stream,
            subscription_id=subscription_id,
            client=client,
            reference=subscriber_name,
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
    ) -> int:
        logger.debug("Consumer subscribe()")
        if offset_specification is None:
            offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)

        async with self._lock:
            logger.debug("[subscribe] Create subscriber for stream: {}".format(stream))
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
                subscriber_id=subscriber.subscription_id,
                handler=partial(self._on_deliver, subscriber=subscriber, filter_value=filter_input),
            )

        logger.debug("[subscribe] Adding handlers for stream: {}".format(stream))
        subscriber.client.add_handler(
            schema.Deliver,
            partial(self._on_deliver, subscriber=subscriber, filter_value=filter_input),
            name=str(subscriber.subscription_id),
        )

        subscriber.client.add_handler(
            schema.MetadataUpdate,
            partial(self._on_metadata_update),
            name=str(subscriber.subscription_id),
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
                    name=str(subscriber.subscription_id),
                )

        if filter_input is not None:
            logger.debug("[subscribe] Filtering scenario enabled for stream: {}".format(stream))
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

        logger.debug("[subscribe] subscribing to stream: {}".format(stream))
        await subscriber.client.subscribe(
            stream=stream,
            subscription_id=subscriber.subscription_id,
            offset_spec=schema.OffsetSpec.from_params(
                offset_specification.offset_type, offset_specification.offset
            ),
            initial_credit=initial_credit,
            properties=properties,
        )
        logger.debug("[subscribe] created for: {}, id: {}".format(stream, subscriber.subscription_id))
        return subscriber.subscription_id

    async def get_available_id(self) -> int:

        # as first loop we always go ahead with the ID to avoid race condition
        # during the reassign of the ID. We have seen it in some edge cases with the
        # other clients
        if self._last_subscriber_id.value < MAX_ITEM_ALLOWED - 1:
            return self._last_subscriber_id.inc()

        # if the last id is greater than MAX_ITEM_ALLOWED
        # we need to loop through all the IDs to find an available one
        for subscribing_id in range(0, MAX_ITEM_ALLOWED - 1):
            if subscribing_id not in self._subscribers:
                return self._last_subscriber_id.swap_value(subscribing_id)

        # if we reach this point it means we have reached the max limit of subscribers
        # each subscriber has a unique ID and can't be > MAX_ITEM_ALLOWED
        raise exceptions.MaxConsumersPerInstance("Max consumers per connection reached")

    async def unsubscribe(self, subscriber_id: int) -> None:
        logger.debug("[unsubscribe] UnSubscribing and removing handlers")
        subscriber = self._subscribers[subscriber_id]

        await subscriber.client.stop_queue_listener_task(subscriber_id=subscriber_id)
        subscriber.client.remove_handler(
            schema.Deliver,
            name=str(subscriber.subscription_id),
        )
        subscriber.client.remove_handler(
            schema.MetadataUpdate,
            name=str(subscriber.subscription_id),
        )
        try:
            await asyncio.wait_for(subscriber.client.unsubscribe(subscriber.subscription_id), 5)
        except asyncio.TimeoutError:
            logger.warning(
                "[unsubscribe] timeout when closing consumer and deleting it for stream: %s",
                subscriber.stream,
            )
        except BaseException as exc:
            logger.warning(" [unsubscribe] exception in unsubscribe of Consumer: %s", exc)

        stream = subscriber.stream

        if stream in self._clients:
            await self._remove_stream_from_client(stream)

        del self._subscribers[subscriber_id]

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
                yield offset, message

        subscriber.offset = frame.chunk_first_offset + frame.num_entries

    async def _on_deliver(
        self, frame: schema.Deliver, subscriber: _Subscriber, filter_value: Optional[FilterConfiguration]
    ) -> None:
        if frame.subscription_id != subscriber.subscription_id:
            return

        await subscriber.client.credit(subscriber.subscription_id, 1)

        for offset, message in self._filter_messages(frame, subscriber, filter_value):
            message_context = MessageContext(
                self,
                subscriber.stream,
                subscriber.subscription_id,
                subscriber.reference,
                offset,
                frame.timestamp,
            )

            maybe_coro = subscriber.callback(subscriber.decoder(message), message_context)
            if maybe_coro is not None:
                await maybe_coro

    async def _on_metadata_update(self, frame: schema.MetadataUpdate) -> None:
        if frame.metadata_info.stream not in self._clients:
            return
        logger.debug(
            "[on_metadata_update] On metadata update event triggered on consumer for stream: %s",
            frame.metadata_info.stream,
        )

        if self._on_close_handler is not None:
            result = self._on_close_handler(
                OnClosedErrorInfo("Metadata Update", [frame.metadata_info.stream])
            )
            if result is not None and inspect.isawaitable(result):
                await result

        await self.maybe_restart_subscriber("Metadata Update", frame.metadata_info.stream)

    async def _on_connection_closed(self, disconnection_info: OnClosedErrorInfo) -> None:
        # clone on_closed_info to avoid modification during iteration
        new_disconnection_info = OnClosedErrorInfo(
            reason=disconnection_info.reason,
            streams=list(disconnection_info.streams) if disconnection_info.streams else [],
        )

        if self._on_close_handler is not None:
            result = self._on_close_handler(new_disconnection_info)
            if result is not None and inspect.isawaitable(result):
                await result

        for stream in disconnection_info.streams.copy():
            reason = disconnection_info.reason
            await self.maybe_restart_subscriber(reason, stream)

    async def clean_list(self, stream: str) -> None:
        async with self._lock:
            current_subscriber = await self._get_subscriber_by_stream(stream)
            if current_subscriber is not None:
                del self._subscribers[current_subscriber.subscription_id]

    async def maybe_restart_subscriber(self, reason: str, stream: str):
        async with self._lock:
            current_subscriber = await self._get_subscriber_by_stream(stream)
            if current_subscriber is not None:
                del self._subscribers[current_subscriber.subscription_id]

        if current_subscriber is not None:
            await self._remove_stream_from_client(stream)
            result = self._recovery_strategy.recover(
                self,
                current_subscriber.stream,
                error=Exception(reason),
                attempt=1,
                # fmt: off
                recovery_fun=lambda stream_s=current_subscriber.stream,
                                    reference=current_subscriber.reference,
                                    callback=current_subscriber.callback,
                                    decoder=current_subscriber.decoder,
                                    offset=current_subscriber.offset,
                                    filter_input=current_subscriber.filter_input:
                self.subscribe(
                    stream=stream_s,
                    subscriber_name=reference,
                    callback=callback,
                    decoder=decoder,
                    offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, offset),
                    filter_input=filter_input,
                ),
                # fmt: on
            )
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
            event_context = EventContext(
                self, subscriber.stream, subscriber.subscription_id, subscriber.reference, reference
            )
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
                del self._subscribers[subscriber.subscription_id]

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

    async def _remove_stream_from_client(self, stream: str) -> None:
        if stream in self._clients:
            await self._clients[stream].remove_stream(stream)
            await self._clients[stream].free_available_id()
            if await self._clients[stream].get_stream_count() == 0:
                await self._clients[stream].close()
            del self._clients[stream]

    async def _get_subscriber_by_stream(self, stream: str) -> Optional[_Subscriber]:
        for subscriber in self._subscribers.values():
            if stream == subscriber.stream:
                return subscriber
        return None

    async def _maybe_clean_up_during_lost_connection(self, stream: str) -> Optional[int]:
        offset = None
        curr_subscriber = await self._get_subscriber_by_stream(stream)
        if curr_subscriber is not None:
            offset = curr_subscriber.offset

        if stream in self._clients:
            await self._remove_stream_from_client(stream)

        return offset
