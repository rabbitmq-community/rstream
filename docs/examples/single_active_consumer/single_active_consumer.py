# rstream: python stream client for rabbitmq stream protocol
# rstream super stream with single active consumer example
# super stream documentation: https://www.rabbitmq.com/docs/streams#super-streams
# single active consumer documentation:
# https://www.rabbitmq.com/blog/2022/07/05/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams
# example path: https://github.com/rabbitmq-community/rstream/blob/master/docs/examples/single_active_consumer/single_active_consumer.py
# more info about rabbitmq stream protocol: https://www.rabbitmq.com/docs/stream

import asyncio
import logging
import signal
from collections import defaultdict

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    EventContext,
    MessageContext,
    OffsetNotFound,
    OffsetSpecification,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
)

cont = 0
lock = asyncio.Lock()

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig()


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global cont
    global lock

    consumer = message_context.consumer
    # you should NOT store the offset every message received in production
    # it could be a performance issue
    # this is just an example
    if message_context.subscriber_name is not None:
        await consumer.store_offset(
            stream=message_context.stream,
            offset=message_context.offset,
            subscriber_name=message_context.subscriber_name,
        )
    logging.info(
        "Got message: {} from stream {} offset {}".format(msg, message_context.stream, message_context.offset)
    )


# We can decide a strategy to manage Offset specification in single active consumer based on is_active flag
# this function will be called every time the active consumer changes
# for example when the current active consumer goes down
# or when a new consumer joins the consumer group
# in this example we will resume from the last stored offset when we become active
async def consumer_update_handler_offset(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    if event_context.subscriber_name is not None:
        logging.info(
            "stream is: " + event_context.stream + " subscriber_name" + event_context.subscriber_name
        )

    if is_active:
        logging.info("I am the active consumer now for the stream:{}".format(event_context.stream))
        try:
            # we are active, we can continue from the last stored offset
            off = await event_context.consumer.query_offset(
                stream=event_context.stream, subscriber_name="consumer-group-1"
            )
            logging.info("Resuming from stored offset: {}, stream: {}".format(off, event_context.stream))
            return OffsetSpecification(OffsetType.OFFSET, off)
        except OffsetNotFound:
            logging.info(
                "No stored offset found, starting from the beginning for the stream:{}".format(
                    event_context.stream
                )
            )
            return OffsetSpecification(OffsetType.OFFSET, 0)
    return OffsetSpecification(OffsetType.OFFSET, 0)


async def consume():
    try:
        logging.info("Starting Super Stream Consumer")
        consumer = SuperStreamConsumer(
            host="localhost",
            port=5552,
            vhost="/",
            username="guest",
            password="guest",
            super_stream="invoices",
        )

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

        # properties of the consumer (enabling single active mode)
        properties: dict[str, str] = defaultdict(str)  # type: ignore
        properties["single-active-consumer"] = "true"
        properties["name"] = "consumer-group-1"
        properties["super-stream"] = "invoices"

        await consumer.subscribe(
            callback=on_message,
            offset_specification=ConsumerOffsetSpecification(offset_type=OffsetType.FIRST),
            decoder=amqp_decoder,
            properties=properties,
            subscriber_name="consumer-group-1",
            consumer_update_listener=consumer_update_handler_offset,
        )
        await consumer.run()
    except Exception as e:
        logging.error("Exception: {}".format(e))


# main coroutine
async def main():
    await consume()
    # report a message
    print("Finished")


# run the asyncio program
asyncio.run(main())
