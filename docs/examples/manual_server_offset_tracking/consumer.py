import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetNotFound,
    OffsetType,
    ServerError,
    StreamDoesNotExist,
    amqp_decoder,
)

cont = 0
lock = asyncio.Lock()
STREAM = "my-test-stream"


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    global cont
    global lock

    consumer = message_context.consumer

    print(
        "Got message: {} from stream {}, offset {}".format(
            msg, message_context.stream, message_context.offset
        )
    )

    # store the offset every 1000 messages received
    async with lock:
        cont = cont + 1
        # store the offset every 1000 messages received
        if cont % 1000 == 0:
            if message_context.subscriber_name is not None:
                await consumer.store_offset(
                    stream=message_context.stream,
                    offset=message_context.offset,
                    subscriber_name=message_context.subscriber_name,
                )


async def consume():
    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    await consumer.start()
    # catch exceptions if stream or offset for the subscriber name doesn't exist
    my_offset = 1
    try:
        # this one will raise an exception if store_offset wasn't never done before (in other word an offset wasn't
        # previously stored in the server)
        my_offset = await consumer.query_offset(stream=STREAM, subscriber_name="subscriber_1")
    except OffsetNotFound as offset_exception:
        print(f"ValueError: {offset_exception}")

    except StreamDoesNotExist as stream_exception:
        print(f"ValueError: {stream_exception}")
        exit(1)

    except ServerError as e:
        print(f"ValueError: {e}")
        exit(1)

    await consumer.subscribe(
        stream=STREAM,
        subscriber_name="subscriber_1",
        callback=on_message,
        decoder=amqp_decoder,
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, my_offset),
    )
    await consumer.run()


# main coroutine
async def main():
    # schedule the task
    task = asyncio.create_task(consume())
    # suspend a moment
    # wait a moment
    await asyncio.sleep(5)
    # cancel the task
    was_cancelled = task.cancel()

    # report a message
    print("Main done")


# run the asyncio program
asyncio.run(main())
