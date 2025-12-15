# getting started with rstream consumer example
# python rstream client for rabbitmq stream protocol
# example of consuming messages using rstream
#
# path example: https://github.com/rabbitmq-community/rstream/blob/master/docs/examples/basic_consumers/basic_consumer.py
# more info about rabbitmq stream protocol: https://www.rabbitmq.com/docs/stream

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
    # define the callback to process messages
    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        print(
            "Got message: {} from stream {}, offset {}".format(
                msg, message_context.stream, message_context.offset
            )
        )

    # initialize and start the consumer
    await consumer.start()
    #  subscribe to a stream with a callback and decoder
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
    #  run the consumer to start receiving messages
    await consumer.run()


asyncio.run(consume())
