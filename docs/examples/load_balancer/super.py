import os
import json
import logging
import asyncio
import time

from collections import defaultdict

from rstream import (
    AMQPMessage,
    OffsetSpecification,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    SuperStreamCreationOption,
    amqp_decoder,
    EventContext,
    OffsetNotFound,
    StreamDoesNotExist,
    ServerError,
)

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger("rstream")

STREAM_NAME = "hello-super-stream"

RMQ_HOST = "localhost"

BASE_NAME = "super-stream-consumer"
CONSUMER_GROUP = f"{BASE_NAME}-group"

# Each Consumer Group should have 2 single-active-consumers
WORKERS = 2

stats = {}


async def update_stats(
        consumer: str,
        stream: str,
        offset: int,
) -> None:
    global stats

    if consumer not in stats.keys():
        stats[consumer] = {}

    if stream not in stats[consumer]:
        stats[consumer][stream] = {}

    if "total" not in stats[consumer][stream]:
        stats[consumer][stream]["total"] = 0

    if "offsets" not in stats[consumer][stream]:
        stats[consumer][stream]["offsets"] = []

    stats[consumer][stream]["total"] += 1
    stats[consumer][stream]["offsets"].append(offset)


async def print_stats(interval: int = 10) -> None:
    while True:
        await asyncio.sleep(interval)
        stats_str = json.dumps(stats, indent=4)
        logger.info(stats_str)


async def on_message(message: AMQPMessage, context: MessageContext) -> None:
    stream = await context.consumer.stream(context.subscriber_name)
    offset = context.offset

    await update_stats(consumer=context.consumer._connection_name, stream=stream, offset=offset)
    logger.info(
        f"{context.consumer._connection_name}: Received {message} from {stream} at {offset}-- storing stream: {stream}, offset: {offset}, subscriber_name: {context.subscriber_name}")

    await context.consumer.store_offset(
        stream=stream, offset=offset, subscriber_name=context.subscriber_name
    )


async def get_stored_offset(context: EventContext) -> int:
    subscriber_name = context.subscriber_name
    stream_name = context.consumer._subscribers.get(subscriber_name).stream

    try:
        offset = await context.consumer.query_offset(
            stream=stream_name, subscriber_name=subscriber_name
        )
        logger.info(f"Stored Offset Found {offset} stream: {stream_name}, subscriber_name: {subscriber_name}")
        return offset
    except OffsetNotFound:
        pass
    except StreamDoesNotExist:
        logger.error(f"{stream_name=} does not exist")
        exit(1)
    except ServerError as e:
        logger.error(f"ServerError: {e.code}")
        exit(1)
    except Exception as e:
        logger.error(f"Unknown exception: {str(e)}")

    logger.info(
        f"NO Stored Offset Found stream: {stream_name}, subscriber_name: {subscriber_name} . Starting from beginning...")
    return -1


async def consumer_update_handler_offset(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    if is_active:
        offset = await get_stored_offset(event_context)
        if offset >= 0:
            offset += 1
            return OffsetSpecification(OffsetType.OFFSET, offset)
        else:
            return OffsetSpecification(OffsetType.FIRST, 0)

    return OffsetSpecification(OffsetType.NEXT, 0)


async def create_super_stream_consumer() -> SuperStreamConsumer:
    super_stream_creation_opt = SuperStreamCreationOption(n_partitions=5)
    consumer = SuperStreamConsumer(
        host=RMQ_HOST,
        vhost="/",
        username="guest",
        password="guest",
        super_stream=STREAM_NAME,
        super_stream_creation_option=super_stream_creation_opt,
        connection_name=f"{CONSUMER_GROUP}-connection",
        # load_balancer_mode=True,
    )

    return consumer


async def subscribe_consumer():
    consumer = await create_super_stream_consumer()

    properties: dict[str, str] = defaultdict(str)
    properties["single-active-consumer"] = "true"
    properties["name"] = CONSUMER_GROUP
    properties["super-stream"] = STREAM_NAME

    # sub_name = f"{CONSUMER_GROUP}-sub"

    logger.info(f"[{CONSUMER_GROUP}] Starting...")
    await consumer.start()

    offset_spec = OffsetSpecification(OffsetType.FIRST, None)

    logger.info(f"[{CONSUMER_GROUP}] Subscribing to {STREAM_NAME=}...")
    await consumer.subscribe(
        # subscriber_name="sub_name",
        callback=on_message,
        decoder=amqp_decoder,
        properties=properties,
        # offset_specification=offset_spec,
        consumer_update_listener=consumer_update_handler_offset,
    )

    return consumer


async def consume():
    consumer = await subscribe_consumer()

    logger.info(f"[{CONSUMER_GROUP}] Starting consumption...")
    await asyncio.gather(consumer.run(), print_stats())


async def main():
    consumer_task = asyncio.create_task(consume())
    await consumer_task


asyncio.run(main())
