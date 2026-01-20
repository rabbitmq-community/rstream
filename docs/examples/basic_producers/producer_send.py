# getting started with rstream producer - send example
# python rstream client for rabbitmq stream protocol
# example of publish messages using rstream with send method and asynchronous confirmation callback
#
# path example: https://github.com/rabbitmq-community/rstream/blob/master/docs/examples/basic_producers/producer_send.py
# more info about rabbitmq stream protocol: https://www.rabbitmq.com/docs/stream


import asyncio
import time

from rstream import AMQPMessage, Producer, ConfirmationStatus

STREAM = "my-test-stream"
MESSAGES = 1_000_000

# asynchronous confirmation callback
async def on_publish_confirm(
        confirmation: ConfirmationStatus
) -> None:
    if confirmation.is_confirmed:
        print(f"Message with id: {confirmation.message_id} confirmed")
    else:
        print(f"Message with id: {confirmation.message_id} not confirmed")


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for i in range(MESSAGES):
            # send messages one by one. Message format is AMQP 1.0
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
            )
            # send is asynchronous with callback for confirmation
            await producer.send(stream=STREAM, message=amqp_message,
                                on_publish_confirm=on_publish_confirm)

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
