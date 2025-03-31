# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import inspect
import logging
import ssl
import uuid
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from random import randrange
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Generic,
    NoReturn,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from . import exceptions, schema, utils
from .amqp import _MessageProtocol
from .client import Addr, Client, ClientPool
from .compression import (
    CompressionHelper,
    CompressionType,
    ICompressionCodec,
)
from .constants import Key, SlasMechanism
from .exceptions import StreamDoesNotExist
from .utils import OnClosedErrorInfo, RawMessage

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)
message_v1_v2 = T = TypeVar("message_v1_v2")
MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]
CB_F = Annotated[Callable[[MT], Awaitable[Any]], "Message callback type"]


@dataclass
class _Publisher:
    id: int
    reference: str
    stream: str
    sequence: utils.MonotonicSeq
    client: Client


@dataclass
class _MessageNotification(Generic[MessageT]):
    entry: MessageT | ICompressionCodec
    callback: Optional[CB[ConfirmationStatus]] = None
    publisher_name: Optional[str] = None


@dataclass
class ConfirmationStatus:
    message_id: int
    is_confirmed: bool = False
    response_code: int = 0


logger = logging.getLogger(__name__)


class Producer:
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
        max_publishers_by_connection=256,
        default_batch_publishing_delay: float = 3,
        default_context_switch_value: int = 1000,
        connection_name: str = None,
        sasl_configuration_mechanism: SlasMechanism = SlasMechanism.MechanismPlain,
        filter_value_extractor: Optional[CB_F[Any]] = None,
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
        self._publishers: dict[str, _Publisher] = {}
        self._waiting_for_confirm: dict[
            str, dict[asyncio.Future[None] | CB[ConfirmationStatus], set[int]]
        ] = defaultdict(dict)
        self._lock = asyncio.Lock()
        # dictionary [stream][list] of buffered messages to send asynchronously
        self._buffered_messages: dict[str, list] = defaultdict(list)
        self._buffered_messages_lock = asyncio.Lock()
        self.task: asyncio.Task[NoReturn] | None = None
        # Delay After sending the messages on _buffered_messages list
        self._default_batch_publishing_delay = default_batch_publishing_delay
        self._default_context_switch_counter = 0
        self._default_context_switch_value = default_context_switch_value
        # self._on_close_handler = on_close_handler
        self._sasl_configuration_mechanism = sasl_configuration_mechanism
        self._close_called = False
        self._connection_name = connection_name
        self._filter_value_extractor: Optional[CB_F[Any]] = filter_value_extractor
        self.publisher_id = 0
        self._max_publishers_by_connection = max_publishers_by_connection

        if self._connection_name is None:
            self._connection_name = "rstream-producer"

    @property
    async def default_client(self) -> Client:
        if self._default_client is None:
            self._default_client = await self._create_locator_connection()
        return self._default_client

    async def __aenter__(self) -> Producer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._close_called = False
        self._default_client = await self._pool.get(
            connection_closed_handler=self._on_connection_closed,
            connection_name=self._connection_name,
            sasl_configuration_mechanism=self._sasl_configuration_mechanism,
            max_clients_by_connections=self._max_publishers_by_connection,
        )

        # Check if the filtering is supported by the server
        if self._filter_value_extractor is not None:
            await self._check_if_filtering_is_supported()

    async def close(self) -> None:
        logger.debug("Closing producer and cleaning up")
        # check if we are in a server disconnection situation:
        # in this case we need avoid other send
        # otherwise if is a normal close() we need to send the last item in batch
        for publisher in self._publishers.values():
            if publisher.client.is_connection_alive() is False:
                self._close_called = True
                # just in this special case give time to all the tasks to complete
                await asyncio.sleep(0.2)
                break

        logger.debug("close(): Stopping background ingestion task and publish pending items")
        if self.task is not None:
            self.task.cancel()
            for stream in self._buffered_messages:
                await self._publish_buffered_messages(stream)

        self._close_called = True

        logger.debug("close(): Deleting publishers and removing handlers")
        for publisher in list(self._publishers.values()):
            if publisher.client.is_connection_alive():
                try:
                    await asyncio.wait_for(publisher.client.delete_publisher(publisher.id), 5)
                except asyncio.TimeoutError:
                    logger.warning("timeout when closing producer and deleting publisher")
                except BaseException:
                    logger.exception("exception in delete_publisher in Producer.close")
            publisher.client.remove_handler(schema.PublishConfirm, publisher.reference)
            publisher.client.remove_handler(schema.PublishError, publisher.reference)
            publisher.client.remove_handler(schema.MetadataUpdate, publisher.reference)

        self._publishers.clear()

        logger.debug("close(): Cleaning up structures")
        await self._pool.close()
        self._clients.clear()
        self._waiting_for_confirm.clear()
        self._default_client = None

    async def _get_or_create_client(self, stream: str) -> Client:
        logger.debug("_get_or_create_client()")
        if stream not in self._clients:
            if self._default_client is None or self._default_client.is_connection_alive() is False:
                logger.debug("_get_or_create_client(): Creating locator connection")
                self._default_client = await self._pool.get(
                    connection_closed_handler=self._on_connection_closed,
                    connection_name=self._connection_name,
                    max_clients_by_connections=self._max_publishers_by_connection,
                )
            leader, _ = await (await self.default_client).query_leader_and_replicas(stream)

            logger.debug("_get_or_create_client(): Getting/Creating new connection")
            self._clients[stream] = await self._pool.get(
                connection_name=self._connection_name,
                addr=Addr(leader.host, leader.port),
                connection_closed_handler=self._on_connection_closed,
                stream=stream,
                sasl_configuration_mechanism=self._sasl_configuration_mechanism,
                max_clients_by_connections=self._max_publishers_by_connection,
            )

            await self._close_locator_connection()

        return self._clients[stream]

    async def _get_or_create_publisher(
        self,
        stream: str,
        publisher_name: Optional[str] = None,
    ) -> _Publisher:
        if stream in self._publishers:
            publisher = self._publishers[stream]
            if publisher_name is not None:
                assert publisher.reference == publisher_name
            return publisher
        try:
            logger.debug("_get_or_create_publisher(): Getting/Creating new publisher")
            client = await self._get_or_create_client(stream)

            # We can have multiple publishers sharing same connection, so their ids must be distinct
            publisher_id = await client.get_available_id()

            reference = publisher_name or f"{stream}_publisher_{publisher_id}_{str(uuid.uuid4())}"
            publisher = self._publishers[stream] = _Publisher(
                id=publisher_id,
                stream=stream,
                reference=reference,
                sequence=utils.MonotonicSeq(),
                client=client,
            )

            logger.debug("_get_or_create_publisher(): Declaring new publisher")
            await client.declare_publisher(
                stream=stream,
                reference=publisher.reference,
                publisher_id=publisher.id,
            )

            sequence = await client.query_publisher_sequence(
                stream=stream,
                reference=reference,
            )
            publisher.sequence.set(sequence + 1)

        except StreamDoesNotExist as e:
            await self._maybe_clean_up_during_lost_connection(stream)
            logger.exception("Error in _get_or_create_publisher: stream does not exists anymore")
            raise e
        except Exception as ex:
            await self._maybe_clean_up_during_lost_connection(stream)
            logger.exception("error declaring publisher")
            raise ex

        logger.debug("_get_or_create_publisher(): Adding handlers")
        client.add_handler(
            schema.PublishConfirm,
            partial(self._on_publish_confirm, publisher=publisher),
            name=publisher.reference,
        )
        client.add_handler(
            schema.PublishError,
            partial(self._on_publish_error, publisher=publisher),
            name=publisher.reference,
        )
        client.add_handler(
            schema.MetadataUpdate,
            partial(self._on_metadata_update),
            name=publisher.reference,
        )

        return publisher

    async def send_batch(
        self,
        stream: str,
        batch: Sequence[MessageT],
        publisher_name: Optional[str] = None,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ) -> list[int]:
        logger.debug("Sending synchronously with _send_batch()")

        if len(batch) == 0:
            raise ValueError("Empty batch")

        if self._close_called:
            return []

        return await self._send_batch(
            stream=stream, batch=batch, publisher_name=publisher_name, callback=on_publish_confirm, sync=False
        )

    # used by send_batch and send_wait
    async def _send_batch(
        self,
        stream: str,
        batch: Sequence[MessageT],
        publisher_name: Optional[str] = None,
        callback: Optional[CB[ConfirmationStatus]] = None,
        sync: bool = True,
        timeout: Optional[int] = None,
    ) -> list[int]:
        logger.debug("Internal _send_batch()")
        messages = []
        publishing_ids = set()

        async with self._lock:
            logger.debug("_send_batch: Creating publisher")
            publisher = await self._get_or_create_publisher(stream, publisher_name=publisher_name)

        for item in batch:
            msg = RawMessage(item) if isinstance(item, bytes) else item

            if msg.publishing_id is None:
                msg.publishing_id = publisher.sequence.next()

            if self._filter_value_extractor is None:
                logger.debug("_send_batch: Not a Filtering scenario appending to messages list")
                messages.append(
                    schema.Message(
                        publishing_id=msg.publishing_id,
                        filter_value=None,
                        data=bytes(msg),
                    )
                )
            else:
                logger.debug("_send_batch: Filtering scenario calling _filter_value_extractor")
                value_filter: str = await self._filter_value_extractor(msg)
                messages.append(
                    schema.Message(
                        publishing_id=msg.publishing_id,
                        filter_value=value_filter,
                        data=bytes(msg),
                    )
                )

        if len(messages) > 0:
            if self._filter_value_extractor is None:
                logger.debug("_send_batch: Calling send_publish_frame version 1")
                await publisher.client.send_publish_frame(
                    schema.Publish(
                        publisher_id=publisher.id,
                        messages=messages,
                    ),
                )

            else:
                logger.debug("_send_batch: Filtering scenario Calling send_publish_frame version 2")
                await publisher.client.send_publish_frame(
                    schema.Publish(
                        publisher_id=publisher.id,
                        messages=messages,
                    ),
                    version=2,
                )

            publishing_ids.update([m.publishing_id for m in messages])

        if not sync:
            logger.debug("_send_batch: Not sync case")
            if callback is not None:
                if callback not in self._waiting_for_confirm[publisher.reference]:
                    self._waiting_for_confirm[publisher.reference][callback] = set()

                self._waiting_for_confirm[publisher.reference][callback].update(publishing_ids)

        # this is just called in case of send_wait
        else:
            logger.debug("_send_batch: sync case")
            future: asyncio.Future[None] = asyncio.Future()
            self._waiting_for_confirm[publisher.reference][future] = publishing_ids.copy()
            await asyncio.wait_for(future, timeout)

        return list(publishing_ids)

    # used by send, send_sub_batch (with compression)
    async def _send_batch_async(
        self,
        stream: str,
        batch: list[_MessageNotification],
    ) -> list[int]:
        logger.debug("Internal _send_batch_async()")

        if len(batch) == 0:
            raise ValueError("Empty batch")

        if self._close_called:
            return []

        messages = []
        publishing_ids = set()
        publishing_ids_callback: dict[CB[ConfirmationStatus], set[int]] = defaultdict(set)

        for item in batch:
            async with self._lock:
                try:
                    logger.debug("_send_batch_async: Getting or Creating publisher")
                    publisher = await self._get_or_create_publisher(
                        stream, publisher_name=item.publisher_name
                    )
                except Exception as ex:
                    raise ex

            if not isinstance(item.entry, ICompressionCodec):
                logger.debug("_send_batch_async: Normal case not Compression case")
                msg = RawMessage(item.entry) if isinstance(item.entry, bytes) else item.entry

                if msg.publishing_id is None:
                    msg.publishing_id = publisher.sequence.next()
                if self._filter_value_extractor is None:
                    logger.debug("_send_batch_async: Filtering not active ingesting messages list")
                    messages.append(
                        schema.Message(
                            publishing_id=msg.publishing_id,
                            filter_value=None,
                            data=bytes(msg),
                        )
                    )
                else:
                    logger.debug("_send_batch_async: Filtering active ingesting messages list")
                    value_filter: str = await self._filter_value_extractor(msg)
                    messages.append(
                        schema.Message(
                            publishing_id=msg.publishing_id,
                            filter_value=value_filter,
                            data=bytes(msg),
                        )
                    )

                if item.callback is not None:
                    publishing_ids_callback[item.callback].add(msg.publishing_id)

            else:
                logger.debug("_send_batch_async: Compression case")
                compression_codec = item.entry
                if len(messages) > 0:
                    if self._filter_value_extractor is None:
                        logger.debug("_send_batch_async: Not a filtering case publishing frame")
                        await publisher.client.send_publish_frame(
                            schema.Publish(
                                publisher_id=publisher.id,
                                messages=messages,
                            ),
                        )
                    else:
                        logger.debug("_send_batch_async: Filtering case ingesting messages list")
                        value_filter = await self._filter_value_extractor(msg)
                        messages.append(
                            schema.Message(
                                publishing_id=msg.publishing_id,
                                filter_value=value_filter,
                                data=bytes(msg),
                            )
                        )
                    # publishing_ids.update([m.publishing_id for m in messages])
                    messages.clear()
                for _ in range(item.entry.messages_count()):
                    publishing_id = publisher.sequence.next()

                logger.debug("_send_batch_async: PublishSubBatching")
                await publisher.client.send_frame(
                    schema.PublishSubBatching(
                        publisher_id=publisher.id,
                        number_of_root_messages=1,
                        publishing_id=publishing_id,
                        compress_type=0x80 | compression_codec.compression_type() << 4,
                        subbatching_message_count=compression_codec.messages_count(),
                        uncompressed_data_size=compression_codec.uncompressed_size(),
                        compressed_data_size=compression_codec.compressed_size(),
                        messages=compression_codec.data(),
                    ),
                )
                publishing_ids.update([publishing_id])

                if item.callback is not None:
                    publishing_ids_callback[item.callback].add(publishing_id)

        if len(messages) > 0:
            if self._filter_value_extractor is None:
                logger.debug("_send_batch_async: Not a Filtering case send_publish_frame v1")
                await publisher.client.send_publish_frame(
                    schema.Publish(
                        publisher_id=publisher.id,
                        messages=messages,
                    ),
                )
            else:
                logger.debug("_send_batch_async: Filtering case send_publish_frame v2")
                await publisher.client.send_publish_frame(
                    schema.Publish(
                        publisher_id=publisher.id,
                        messages=messages,
                    ),
                    version=2,
                )
            publishing_ids.update([m.publishing_id for m in messages])

        for callback in publishing_ids_callback:
            if callback not in self._waiting_for_confirm[publisher.reference]:
                self._waiting_for_confirm[publisher.reference][callback] = set()

            self._waiting_for_confirm[publisher.reference][callback].update(publishing_ids_callback[callback])

        return list(publishing_ids)

    async def send_wait(
        self,
        stream: str,
        message: MessageT,
        publisher_name: Optional[str] = None,
        timeout: Optional[int] = 5,
    ) -> int:
        logger.debug("Sending synchronously with send_wait")
        publishing_ids = await self._send_batch(
            stream,
            [message],
            publisher_name=publisher_name,
            sync=True,
            timeout=timeout,
        )
        return publishing_ids[0]

    def _timer_completed(self, context):
        logger.debug("Background ingestion task completed")
        if not context.cancelled():
            if context.exception():
                raise context.exception()

        return 0

    async def send(
        self,
        stream: str,
        message: MessageT,
        publisher_name: Optional[str] = None,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ):
        logger.debug("Sending asynchronously with send()")
        # start the background thread to send buffered messages
        if self.task is None:
            logger.debug("Creating background task")
            self.task = asyncio.create_task(self._timer())
            self.task.add_done_callback(self._timer_completed)

        wrapped_message = _MessageNotification(
            entry=message, callback=on_publish_confirm, publisher_name=publisher_name
        )
        async with self._buffered_messages_lock:
            logger.debug("Send(): Appending to buffer")
            self._buffered_messages[stream].append(wrapped_message)

        self._default_context_switch_counter += 1

        if self._default_context_switch_counter > self._default_context_switch_value:
            await asyncio.sleep(0)
            self._default_context_switch_counter = 0

    async def send_sub_entry(
        self,
        stream: str,
        sub_entry_messages: Sequence[MessageT],
        compression_type: CompressionType = CompressionType.No,
        publisher_name: Optional[str] = None,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ):
        logger.debug("Sending asynchronously with send_sub_entry()")

        if len(sub_entry_messages) == 0:
            raise ValueError("Empty batch")

        if self._filter_value_extractor is not None:
            raise ValueError("filtering can't be enabled in sub-batching mode")

        # start the background thread to send buffered messages
        if self.task is None:
            logger.debug("Creating background task")
            self.task = asyncio.create_task(self._timer())
            self.task.add_done_callback(self._timer_completed)

        compression_codec = CompressionHelper.compress(sub_entry_messages, compression_type)

        wrapped_message = _MessageNotification(
            entry=compression_codec, callback=on_publish_confirm, publisher_name=publisher_name
        )
        async with self._buffered_messages_lock:
            logger.debug("send_sub_entry(): Appending to buffer")
            self._buffered_messages[stream].append(wrapped_message)

        await asyncio.sleep(0)

    # After the timeout send the messages in _buffered_messages in batches
    async def _timer(self):
        logger.debug("Background timer task created")
        try:
            while not self._close_called:
                logger.debug("Background timer task looping for ingestion")
                await asyncio.sleep(0.5)
                for stream in self._buffered_messages:
                    try:
                        await self._publish_buffered_messages(stream)
                    except BaseException:
                        logger.exception("producer _timer exception")
        except Exception:
            logger.exception("exception in _timer")

    async def _publish_buffered_messages(self, stream: str) -> None:
        logger.debug("publishing message with _publish_buffered_messages")
        async with self._buffered_messages_lock:
            if len(self._buffered_messages[stream]):
                await self._send_batch_async(stream, self._buffered_messages[stream])
                self._buffered_messages[stream].clear()

    async def _on_publish_confirm(self, frame: schema.PublishConfirm, publisher: _Publisher) -> None:
        logger.debug("_on_publish_confirm callback: waiting for confirmations")
        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for confirmation in list(waiting):
            logger.debug("_on_publish_confirm: looping over confirmations")
            ids = waiting[confirmation]
            ids_to_call = ids.intersection(set(frame.publishing_ids))

            for id in ids_to_call:
                if not isinstance(confirmation, asyncio.Future):
                    confirmation_status: ConfirmationStatus = ConfirmationStatus(id, True)
                    result = confirmation(confirmation_status)
                    if result is not None and hasattr(result, "__await__"):
                        await result
            ids.difference_update(frame.publishing_ids)
            if not ids:
                logger.debug("_on_publish_confirm: set empty setting future result")
                del waiting[confirmation]
                if isinstance(confirmation, asyncio.Future):
                    confirmation.set_result(None)

    async def _on_publish_error(self, frame: schema.PublishError, publisher: _Publisher) -> None:
        if frame.publisher_id != publisher.id:
            return

        waiting = self._waiting_for_confirm[publisher.reference]
        for error in frame.errors:
            exc = exceptions.ServerError.from_code(error.response_code)
            for confirmation in list(waiting):
                ids = waiting[confirmation]
                if error.publishing_id in ids:
                    if not isinstance(confirmation, asyncio.Future):
                        confirmation_status = ConfirmationStatus(
                            error.publishing_id, False, error.response_code
                        )
                        result = confirmation(confirmation_status)
                        if result is not None and inspect.isawaitable(result):
                            await result
                        ids.remove(error.publishing_id)

                if not ids:
                    del waiting[confirmation]
                    if isinstance(confirmation, asyncio.Future):
                        confirmation.set_exception(exc)

    async def _on_metadata_update(self, frame: schema.MetadataUpdate) -> None:
        logger.debug("_on_metadata_update: On metadata update event triggered on producer")
        async with self._lock:
            await self._maybe_clean_up_during_lost_connection(frame.metadata_info.stream)

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

    async def clean_up_publishers(self, stream: str):
        if stream in self._publishers:
            publisher = self._publishers[stream]
            await publisher.client.delete_publisher(publisher.id)
            publisher.client.remove_handler(schema.PublishConfirm, publisher.reference)
            publisher.client.remove_handler(schema.PublishError, publisher.reference)
            publisher.client.remove_handler(schema.MetadataUpdate, publisher.reference)
            del self._publishers[stream]

    async def delete_stream(self, stream: str, missing_ok: bool = False) -> None:
        await self.clean_up_publishers(stream)

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
            stream_exist = await (await self.default_client).stream_exists(stream)
            await self._close_locator_connection()
        return stream_exist

    async def _check_if_filtering_is_supported(self) -> None:
        logger.debug("_check_if_filtering_is_supported")
        command_version_input = schema.FrameHandlerInfo(Key.Publish.value, min_version=1, max_version=2)
        server_command_version: schema.FrameHandlerInfo = await (
            await self.default_client
        ).exchange_command_version(command_version_input)
        if await (await self.default_client).get_stream_count() == 0:
            await (await self.default_client).close()
            self._default_client = None

        if server_command_version.max_version < 2:
            filter_not_supported = (
                "Filtering is not supported by the broker "
                + "(requires RabbitMQ 3.13+ and stream_filtering feature flag activated)"
            )
            raise ValueError(filter_not_supported)

    async def _create_locator_connection(self) -> Client:
        return await self._pool.get(
            connection_closed_handler=self._on_connection_closed,
            connection_name=self._connection_name,
            sasl_configuration_mechanism=self._sasl_configuration_mechanism,
        )

    async def _close_locator_connection(self):
        if await (await self.default_client).get_stream_count() == 0:
            await (await self.default_client).close()
            self._default_client = None

    async def _maybe_clean_up_during_lost_connection(self, stream: str):
        logger.debug(
            "_maybe_clean_up_during_lost_connection: Cleaning after disconnection or metaata update events"
        )

        await asyncio.sleep(randrange(3))

        if stream in self._publishers:
            # try to delete the publisher if deadling
            try:
                await asyncio.wait_for(
                    self._publishers[stream].client.delete_publisher(self._publishers[stream].id), 3
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout deleting publisher in maybe_clean_up_during_lost_connection")
            if self._publishers[stream].client.is_connection_alive():
                await self._publishers[stream].client.remove_stream(stream)
                await self._publishers[stream].client.free_available_id(self._publishers[stream].id)
                if await self._publishers[stream].client.get_stream_count() == 0:
                    await self._publishers[stream].client.close()
            else:
                await self._publishers[stream].client.close()
            del self._publishers[stream]

        if stream in self._clients:
            del self._clients[stream]

    # this is notified by the client when a disconnection happens in order to cleanup handlers
    async def _on_connection_closed(self, disconnection_info: OnClosedErrorInfo) -> None:
        logger.debug("_on_connection_closed: Notification of socket disconnections")
        async with self._lock:
            for stream in disconnection_info.streams:
                await self._maybe_clean_up_during_lost_connection(stream)
