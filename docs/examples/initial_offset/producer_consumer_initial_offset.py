"""Create a stream whose first message does not start at offset 0.

Requires RabbitMQ 4.4.0 or later (the `stream-initial-offset` stream argument).
The initial offset is fixed when the stream is created and must be in [0, 2**62 - 1];
a `ValueError` is raised client-side for values outside that range.
"""

import asyncio

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    Producer,
    amqp_decoder,
)

STREAM = "my-initial-offset-stream"
INITIAL_OFFSET = 1_000
MESSAGES = 5


async def run():
    consumed = asyncio.Event()
    received = 0

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        nonlocal received
        # the first message has offset == INITIAL_OFFSET, the following ones increase by one
        print(f"Got message: {msg} at offset: {message_context.offset}")
        received += 1
        if received == MESSAGES:
            consumed.set()

    async with Producer("localhost", username="guest", password="guest") as producer:
        # the stream must not already exist with a different initial offset
        await producer.create_stream(STREAM, exists_ok=True, initial_offset=INITIAL_OFFSET)

        async with Consumer("localhost", username="guest", password="guest") as consumer:
            await consumer.subscribe(STREAM, callback=on_message, decoder=amqp_decoder)

            for i in range(MESSAGES):
                await producer.send_wait(STREAM, AMQPMessage(body=f"hello: {i}".encode()))

            await asyncio.wait_for(consumed.wait(), timeout=10)

        await producer.delete_stream(STREAM)


asyncio.run(run())
