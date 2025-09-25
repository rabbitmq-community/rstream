import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    FilterConfiguration,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"
cont = 0


async def consume():
    filters = ["New York"]

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
        print("Application property: " + str(msg.application_properties[b"region"]))
        global cont
        cont = cont + 1
        # we should read only 200 messages
        print("cont: " + str(cont))

    await consumer.start()
    await consumer.subscribe(
        stream=STREAM,
        callback=on_message,
        decoder=amqp_decoder,
        filter_input=FilterConfiguration(
            values_to_filter=filters,
            # this post filter can be used to filter messages client side.
            # predicate=lambda message: message.application_properties[b"region"] == filters[0].encode("utf-8"),
            match_unfiltered=False,
        ),
    )
    await consumer.run()


asyncio.run(consume())
