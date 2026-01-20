import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"


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

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        print(
            "Got message: {} from stream {}, offset {}".format(
                msg, message_context.stream, message_context.offset
            )
        )

    await consumer.subscribe(stream=STREAM, callback=on_message)
    await consumer.run()


asyncio.run(consume())
