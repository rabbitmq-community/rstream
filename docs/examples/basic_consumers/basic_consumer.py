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
        consumer = message_context.consumer
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset

        print("Got message: {}".format(msg) + " from stream " + stream + " offset: " + str(offset))

    await consumer.start()
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
    await consumer.run()


asyncio.run(consume())