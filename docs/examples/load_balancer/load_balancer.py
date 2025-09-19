import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    SuperStreamConsumer,
    amqp_decoder,
)

STREAM = "my-test-load-balancer-stream"

import logging

logging.getLogger("rstream").setLevel(logging.DEBUG)


async def consume():
    consumer = SuperStreamConsumer(
        host="192.168.1.52",
        port=5552,
        vhost="/",
        username="test",
        password="test",
        super_stream=STREAM,
        load_balancer_mode=True,
    )
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))
    logging.info("Starting consumer")

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    await consumer.create_super_stream(STREAM, exists_ok=True, n_partitions=3)
    await consumer.start()
    await consumer.subscribe(callback=on_message, decoder=amqp_decoder)
    await consumer.run()
    ### sleep forever


asyncio.run(consume())
