# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import abc
import logging
from dataclasses import dataclass
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
)

import mmh3

from .amqp import _MessageProtocol
from .client import Client

MessageT = TypeVar("MessageT", _MessageProtocol, bytes)

logger = logging.getLogger(__name__)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], Awaitable[Any]], "Message callback type"]


class Metadata(abc.ABC):
    @abc.abstractmethod
    async def partitions(self) -> list[str]:
        pass

    @abc.abstractmethod
    async def routes(self, routing_key: str) -> list[str]:
        pass


class DefaultSuperstreamMetadata(Metadata):
    def __init__(self, super_stream: str, client: Client):
        self.super_stream = super_stream
        self.client = client
        self._partitions: list[str] = []
        self._routes: list[str] = []

    async def partitions(self) -> list[str]:
        logger.debug("partitions() Get Partitions from server")
        if len(self._partitions) == 0:
            self._partitions = await self.client.partitions(self.super_stream)
            if len(self._partitions) <= 0:
                raise ValueError(
                    "the number of partitions of the stream is <= to 0, the superstream doesn't probably exist"
                )
            # locator not necessary anymore
            await self.client.close()

        return self._partitions

    async def routes(self, routing_key: str) -> list[str]:
        if len(self._routes) == 0:
            self._routes = await self.client.route(routing_key, self.super_stream)
        return self._routes


class RoutingStrategy(abc.ABC):
    async def route(self, message: MessageT, metadata: Metadata) -> list[str]:
        pass


class RoutingKeyRoutingStrategy(RoutingStrategy):
    def __init__(self, routingKeyExtractor: CB[Any]):
        self.routingKeyExtractor: CB[Any] = routingKeyExtractor

    async def route(self, message: MessageT, metadata: Metadata) -> list[str]:
        key = await self.routingKeyExtractor(message)
        return await metadata.routes(str(key))


class HashRoutingMurmurStrategy(RoutingStrategy):
    def __init__(self, routingKeyExtractor: CB[Any]):
        self.routingKeyExtractor: CB[Any] = routingKeyExtractor

    async def route(self, message: MessageT, metadata: Metadata) -> list[str]:
        logger.debug("route() Compute routing")
        streams = []
        key = await self.routingKeyExtractor(message)
        key_bytes = bytes(key, "UTF-16")
        hash = mmh3.hash_bytes(key_bytes, 104729)
        number_of_partitions = len(await metadata.partitions())

        route = int.from_bytes(hash, "little", signed=False) % number_of_partitions

        partitions = await metadata.partitions()
        stream = partitions[route]
        streams.append(stream)

        return streams


@dataclass
class SuperStreamCreationOption:
    n_partitions: int
    binding_keys: Optional[list[str]] = None
    arguments: Optional[dict[str, Any]] = None
