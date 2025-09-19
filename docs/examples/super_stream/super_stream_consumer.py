import asyncio
import signal

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    SuperStreamCreationOption,
    amqp_decoder,
)

cont = 0


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    offset = message_context.offset
    print("Received message: {} from stream: {} - message offset: {}".format(msg, message_context.stream, offset))


async def consume():
    super_stream_creation_opt = SuperStreamCreationOption(n_partitions=3)
    consumer = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        super_stream="invoices",
        super_stream_creation_option=super_stream_creation_opt,
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))
    offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST, None)
    await consumer.start()
    await consumer.subscribe(
        callback=on_message, decoder=amqp_decoder, offset_specification=offset_specification
    )
    await consumer.run()


asyncio.run(consume())
