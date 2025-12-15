# python rstream client for rabbitmq stream protocol
# example of publish messages using rstream with send_wait method for synchronous sending and waiting for confirmation
#
# path example: https://github.com/rabbitmq-community/rstream/blob/master/docs/examples/basic_producers/producer_send_wait.py
# more info about rabbitmq stream protocol: https://www.rabbitmq.com/docs/stream

import asyncio

from rstream import AMQPMessage, Producer

STREAM = "my-test-stream"
MESSAGES = 1000


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a thousand of messages in AMQP format
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
            )
            # send_wait is synchronous. It will also wait synchronously for the confirmation to arrive from the server
            # it is really very slow and send() + callback for asynchronous confirmation should be used instead.
            await producer.send_wait(stream=STREAM, message=amqp_message)


asyncio.run(publish())
