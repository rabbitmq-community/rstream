import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    amqp_decoder, ConsumerOffsetSpecification, OffsetType
)

STREAM = "my-test-stream"
import logging

logging.getLogger("rstream").setLevel(logging.INFO)
logging.info("Starting consumer")


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
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    await consumer.start()
    await consumer.subscribe(stream=STREAM,
                             callback=on_message,
                             decoder=amqp_decoder,
                             offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None))
    await consumer.run()
    # sleep for 10 seconds
    await asyncio.sleep(10)
    logging.info("Stopping consumer")
    consumer.stop()
    logging.info("Consumer stopped")

asyncio.run(consume())
