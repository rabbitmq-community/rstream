# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable, Optional

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    Consumer,
    EventContext,
    MessageContext,
    OffsetSpecification,
    OffsetType,
    Producer,
    SuperStreamConsumer,
    amqp_decoder,
)

from .http_requests import (
    http_api_connection_exists,
    http_api_delete_connection,
    http_api_get_connection,
    http_api_get_connections,
)

captured: list[bytes] = []
logger = logging.getLogger(__name__)


async def wait_for(condition, timeout=1, interval=0.5):
    await asyncio.sleep(interval)

    async def _wait():
        while not condition():
            await asyncio.sleep(interval)

    await asyncio.wait_for(_wait(), timeout)


async def consumer_update_handler_next(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    return OffsetSpecification(OffsetType.NEXT, 0)


async def consumer_update_handler_first(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    return OffsetSpecification(OffsetType.FIRST, 0)


async def on_publish_confirm_client_callback(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:
    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


async def on_publish_confirm_client_callback2(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:
    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


async def routing_extractor(message: AMQPMessage) -> str:
    return "0"


async def routing_extractor_generic(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def routing_extractor_for_sac(message: AMQPMessage) -> str:
    return str(message.properties.message_id)


async def routing_extractor_key(message: AMQPMessage) -> str:
    return "key1"


async def on_message(
    msg: AMQPMessage, message_context: MessageContext, streams: list[str], offsets: list[int]
):
    streams.append(message_context.stream)
    offset = message_context.offset
    offsets.append(offset)


async def on_message_sac(_msg: AMQPMessage, message_context: MessageContext, streams: list[str]):
    streams.append(message_context.stream)


async def run_consumer(
    super_stream_consumer: SuperStreamConsumer,
    streams: list[str],
    consumer_update_listener: Optional[Callable[[bool, EventContext], Awaitable[Any]]] = None,
):
    properties: dict[str, str] = defaultdict(str)
    properties["single-active-consumer"] = "true"
    properties["name"] = "consumer-group-1"
    properties["super-stream"] = "test-super-stream"

    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: streams.append(message_context.stream),
        decoder=amqp_decoder,
        properties=properties,
        consumer_update_listener=consumer_update_listener,
    )


async def http_api_delete_connection_and_check(connection_name: str) -> None:
    # delay a few seconds before deleting the connection
    await asyncio.sleep(2)

    connections = http_api_get_connections()

    await wait_for(lambda: http_api_connection_exists(connection_name, connections) is True, 5, 1)

    for connection in connections:
        if connection["client_properties"]["connection_name"] == connection_name:
            http_api_delete_connection(connection["name"])
            await wait_for(lambda: http_api_get_connection(connection["name"]) is False, 5)


async def delete_stream_from_producer(producer: Producer, stream: str) -> None:
    # delay a few seconds before deleting the connection
    await asyncio.sleep(4)

    await producer.delete_stream(stream)


async def delete_stream_from_consumer(consumer: Consumer, stream: str) -> None:
    # delay a few seconds before deleting the connection
    await asyncio.sleep(4)

    await consumer.delete_stream(stream)


async def filter_value_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]
