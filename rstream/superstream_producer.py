# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import logging
import ssl
from enum import Enum
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
)

from . import exceptions
from .amqp import _MessageProtocol
from .client import Client, ClientPool
from .producer import ConfirmationStatus, Producer
from .superstream import (
    DefaultSuperstreamMetadata,
    HashRoutingMurmurStrategy,
    RoutingKeyRoutingStrategy,
    RoutingStrategy,
    SuperStreamCreationOption,
)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Awaitable[Any]], "Message callback type"]
CB_F = Annotated[Callable[[MT], Awaitable[Any]], "Message callback type"]

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)

logger = logging.getLogger(__name__)


class RouteType(Enum):
    Hash = 0
    Key = 1


class SuperStreamProducer:
    def __init__(
        self,
        host: str,
        port: int = 5552,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        vhost: str = "/",
        username: str,
        password: str,
        super_stream: str,
        super_stream_creation_option: Optional[SuperStreamCreationOption] = None,
        routing_extractor: CB[Any],
        routing: RouteType = RouteType.Hash,
        frame_max: int = 1 * 1024 * 1024,
        heartbeat: int = 60,
        load_balancer_mode: bool = False,
        max_retries: int = 20,
        max_publishers_by_connection=256,
        default_batch_publishing_delay: float = 0.2,
        connection_name: str = "",
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
        )
        self.host = host
        self.port = port
        self.vhost = vhost
        self.username = username
        self.password = password
        self.ssl_context = ssl_context
        self.super_stream = super_stream
        self.routing = routing
        self.routing_extractor: CB[Any] = routing_extractor
        self.frame_max = frame_max
        self.heartbeat = heartbeat
        self.load_balancer_mode = load_balancer_mode
        self.max_retries = max_retries
        self.default_batch_publishing_delay = default_batch_publishing_delay
        self._default_client: Optional[Client] = None
        self._producer: Producer | None = None
        self._routing_strategy: RoutingStrategy
        self._connection_name = connection_name
        if self._connection_name is None or self._connection_name == "":
            self._connection_name = "rstream-producer"
        self._filter_value_extractor: Optional[CB_F[Any]] = filter_value_extractor
        self.super_stream_creation_option = super_stream_creation_option
        # is containing partitions name for every stream in case of CREATE/DELETE superstream (to clean up publishers)
        self._partitions: list = []
        self._max_publishers_by_connection = max_publishers_by_connection

    async def _get_producer(self) -> Producer:
        logger.debug("_get_producer() Making or getting a producer")
        if self._producer is None:
            producer = Producer(
                host=self.host,
                port=self.port,
                vhost=self.vhost,
                username=self.username,
                password=self.password,
                ssl_context=self.ssl_context,
                frame_max=self.frame_max,
                heartbeat=self.heartbeat,
                load_balancer_mode=self.load_balancer_mode,
                default_batch_publishing_delay=self.default_batch_publishing_delay,
                connection_name=self._connection_name,
                filter_value_extractor=self._filter_value_extractor,
                max_publishers_by_connection=self._max_publishers_by_connection,
            )
            await producer.start()
            self._producer = producer
        return self._producer

    async def send(
        self,
        message: MessageT,
        on_publish_confirm: Optional[CB[ConfirmationStatus]] = None,
    ) -> None:
        logger.debug("Send() asynchronously with superstream")
        streams = await self._routing_strategy.route(message, self.super_stream_metadata)
        self._producer = await self._get_producer()

        for stream in streams:
            await self._producer.send(stream=stream, message=message, on_publish_confirm=on_publish_confirm)

    @property
    async def default_client(self) -> Client:
        if self._default_client is None:
            self._default_client = await self._pool.get(connection_name="rstream-locator")
        return self._default_client

    async def __aenter__(self):
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
        self._default_client = await self._pool.get(
            connection_name="rstream-locator", max_clients_by_connections=self._max_publishers_by_connection
        )
        self.super_stream_metadata = DefaultSuperstreamMetadata(self.super_stream, self._default_client)
        if self.routing == RouteType.Hash:
            self._routing_strategy = HashRoutingMurmurStrategy(self.routing_extractor)
        else:
            self._routing_strategy = RoutingKeyRoutingStrategy(self.routing_extractor)

    async def close(self) -> None:
        if self._default_client is not None:
            await self._default_client.close()
            self._default_client = None
        await self._pool.close()
        if self._producer is not None:
            await self._producer.close()

    async def stream_exists(self, stream: str) -> bool:
        producer = await self._get_producer()
        return await producer.stream_exists(stream)

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
        producer = await self._get_producer()

        # if we are trying to delete the super_stream connected to the Superstream Producer clean up publishers
        if super_stream == self.super_stream:
            for partition in self._partitions:
                await producer.clean_up_publishers(partition)
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
