# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import logging
import ssl
from collections import defaultdict
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from . import exceptions
from .amqp import AMQPMessage
from .client import Client, ClientPool
from .constants import (
    MAX_ITEM_ALLOWED,
    ConsumerOffsetSpecification,
    OffsetType,
)
from .consumer import Consumer, EventContext, MessageContext
from .recovery import BackOffRecoveryStrategy, RecoveryStrategy
from .superstream import (
    DefaultSuperstreamMetadata,
    SuperStreamCreationOption,
)
from .utils import FilterConfiguration, OnClosedErrorInfo

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Union[None, Awaitable[None]]], "Message callback type"]

logger = logging.getLogger(__name__)


class SuperStreamConsumer:
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
        max_subscribers_by_connection: int = MAX_ITEM_ALLOWED,
        super_stream: str,
        super_stream_creation_option: Optional[SuperStreamCreationOption] = None,
        connection_name: str = "",
        on_close_handler: Optional[CB[OnClosedErrorInfo]] = None,
        recovery_strategy: RecoveryStrategy = BackOffRecoveryStrategy(),
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
        )
        self._default_client: Optional[Client] = None
        self._clients: dict[str, Client] = {}
        self._lock = asyncio.Lock()
        self.super_stream = super_stream
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.ssl_context = ssl_context
        self.frame_max = frame_max
        self.heartbeat = heartbeat
        self.load_balancer_mode = load_balancer_mode
        self.max_retries = max_retries
        self._consumer: Optional[Consumer] = None
        self._stop_event = asyncio.Event()
        self._subscribers: dict[str, int] = defaultdict(int)
        self._on_close_handler = on_close_handler
        self._connection_name = connection_name
        self._recovery_strategy = recovery_strategy
        if self._connection_name is None or self._connection_name == "":
            self._connection_name = "rstream-consumer"

        self.super_stream_creation_option = super_stream_creation_option
        # is containing partitions name for every stream in case of CREATE/DELETE superstream
        self._partitions: list = []
        self._max_subscribers_by_connection = max_subscribers_by_connection

    @property
    async def default_client(self) -> Client:
        if self._default_client is None:
            self._default_client = await self._pool.get(
                connection_name="rstream-locator",
                max_clients_by_connections=self._max_subscribers_by_connection,
            )
        return self._default_client

    async def __aenter__(self) -> SuperStreamConsumer:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if self.super_stream_creation_option is not None:
            await self.create_super_stream(
                self.super_stream,
                self.super_stream_creation_option.n_partitions,
                self.super_stream_creation_option.binding_keys,
                self.super_stream_creation_option.arguments,
                True,
            )
        self._default_client = None

    def stop(self) -> None:
        self._stop_event.set()

    async def close(self) -> None:
        if self._consumer is not None:
            await self._consumer.close()

        self.stop()
        await self._pool.close()
        self._clients.clear()
        self._default_client = None

    async def run(self) -> None:
        await self._stop_event.wait()

    async def subscribe(
        self,
        callback: Callable[[AMQPMessage, MessageContext], Union[None, Awaitable[None]]],
        *,
        decoder: Optional[Callable[[bytes], MT]] = None,
        offset_specification: Optional[ConsumerOffsetSpecification] = None,
        initial_credit: int = 10,
        properties: Optional[dict[str, Any]] = None,
        subscriber_name: Optional[str] = None,
        consumer_update_listener: Optional[Callable[[bool, EventContext], Awaitable[Any]]] = None,
        filter_input: Optional[FilterConfiguration] = None,
    ):
        logger.debug("Superstream subscribe()")
        if offset_specification is None:
            offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)

        if self._default_client is None or self._default_client.is_connection_alive() is False:
            self._default_client = await self._pool.get(
                connection_closed_handler=self._on_close_handler, connection_name="rstream-locator"
            )

        logger.debug("subscribe(): Get _super_stream_metadata and partitions")
        self._super_stream_metadata = DefaultSuperstreamMetadata(self.super_stream, self._default_client)
        partitions = await self._super_stream_metadata.partitions()

        if self._consumer is None:
            self._consumer = await self._create_consumer()
        for partition in partitions:
            consumer_partition: Optional[Consumer] = self._consumer
            if consumer_partition is None:
                return

            logger.debug("subscribe(): subscribe to a partition")
            subscriber_id = await consumer_partition.subscribe(
                stream=partition,
                callback=callback,
                decoder=decoder,
                offset_specification=offset_specification,
                initial_credit=initial_credit,
                properties=properties,
                subscriber_name=subscriber_name,
                consumer_update_listener=consumer_update_listener,
                filter_input=filter_input,
            )
            self._subscribers[partition] = subscriber_id

    async def _create_consumer(self) -> Consumer:
        logger.debug("_create_consumer(): creating consumer if not exists")
        consumer = Consumer(
            host=self.host,
            port=self.port,
            vhost=self.vhost,
            username=self.username,
            password=self.password,
            ssl_context=self.ssl_context,
            frame_max=self.frame_max,
            heartbeat=self.heartbeat,
            load_balancer_mode=self.load_balancer_mode,
            max_retries=self.max_retries,
            on_close_handler=self._on_close_handler,
            connection_name=self._connection_name,
            max_subscribers_by_connection=self._max_subscribers_by_connection,
            recovery_strategy=self._recovery_strategy,
        )

        await consumer.start()

        return consumer

    async def unsubscribe(self) -> None:
        logger.debug("unsubscribe(): unsubscribe superstream consumer unsubscribe all consumers")
        partitions = await self._super_stream_metadata.partitions()
        for partition in partitions:
            consumer = self._consumer
            if consumer is not None:
                await consumer.unsubscribe(self._subscribers[partition])

    async def stream_exists(self, stream: str) -> bool:
        stream_exist = False
        if self._consumer is not None:
            stream_exist = await self._consumer.stream_exists(stream)
        return stream_exist

    async def create_super_stream(
        self,
        super_stream: str,
        n_partitions: int = 0,
        binding_keys: Optional[list[str]] = None,
        arguments: Optional[dict[str, Any]] = None,
        exists_ok: bool = False,
    ) -> None:
        if binding_keys is not None and n_partitions != 0:
            raise ValueError("Just one between n_partitions and binding_keys can be specified")

        new_binding_key = []
        partitions = []

        if binding_keys is None:
            for i in range(n_partitions):
                partitions.append(super_stream + "-" + str(i))
                new_binding_key.append(str(i))
        else:
            for i in range(len(binding_keys)):
                new_binding_key = binding_keys
                partitions.append(super_stream + "-" + binding_keys[i])

        try:
            await (await self.default_client).create_super_stream(
                super_stream, partitions, new_binding_key, arguments
            )
        except exceptions.StreamAlreadyExists:
            if not exists_ok:
                raise
        finally:
            await self._close_locator_connection()
            if super_stream == self.super_stream:
                self._partitions = partitions

    async def delete_super_stream(self, super_stream: str, missing_ok: bool = False) -> None:
        # if we are deleting the super_stream connected to the SuperstreamConsumer
        # clean up the subscribers
        if super_stream == self.super_stream:
            for partition in self._partitions:
                consumer = self._consumer
                if consumer is not None:
                    await consumer.clean_up_subscribers(partition)
            self._partitions = []

        try:
            await (await self.default_client).delete_super_stream(super_stream)
        except exceptions.StreamDoesNotExist:
            if not missing_ok:
                raise
        finally:
            await self._close_locator_connection()

    async def _close_locator_connection(self):
        if self._default_client is not None:
            if await (await self.default_client).get_stream_count() == 0:
                await (await self.default_client).close()
                self._default_client = None
