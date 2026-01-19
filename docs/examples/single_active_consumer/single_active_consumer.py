import asyncio
import signal
from collections import defaultdict

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    EventContext,
    MessageContext,
    OffsetSpecification,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
)

cont = 0
lock = asyncio.Lock()


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global cont
    global lock

    consumer = message_context.consumer
    # store the offset every message received
    # you should not store the offset every message received in production
    # it could be a performance issue
    # this is just an example
    if message_context.subscriber_name is not None:
        await consumer.store_offset(
            stream=message_context.stream,
            offset=message_context.offset,
            subscriber_name=message_context.subscriber_name,
        )
    print(
        "Got message: {} from stream {} offset {}".format(msg, message_context.stream, message_context.offset)
    )


# We can decide a strategy to manage Offset specification in single active consumer based on is_active flag
# By default if not present the always the strategy OffsetType.NEXT will be set.
# This handle will be passed to subscribe.
async def consumer_update_handler_offset(is_active: bool, event_context: EventContext) -> OffsetSpecification:
    if event_context.subscriber_name is not None:
        print("stream is: " + event_context.stream + " subscriber_name" + event_context.subscriber_name)

    if is_active:
        # Put the logic of your use case here
        return OffsetSpecification(OffsetType.OFFSET, 10)

    return OffsetSpecification(OffsetType.NEXT, 0)


async def consume():
    try:
        print("Starting Super Stream Consumer")
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
            consumer_update_listener=consumer_update_handler_offset,
        )
        await consumer.run()
    except Exception as e:
        print("Exception: {}".format(e))


# main coroutine
async def main():
    # schedule the task
    task = asyncio.create_task(consume())
    # suspend a moment
    # wait a moment
    await asyncio.sleep(20)
    # cancel the task
    task.cancel()

    # report a message
    print("Finished")


# run the asyncio program
asyncio.run(main())
