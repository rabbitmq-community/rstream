# getting started with rstream producer example and send_batch method
# python rstream client for rabbitmq stream protocol
# example of publish messages using rstream with send_batch method and asynchronous confirmation callback
#
# path example: https://github.com/rabbitmq-community/rstream/blob/master/docs/examples/basic_producers/producer_send_batch.py
# more info about rabbitmq stream protocol: https://www.rabbitmq.com/docs/stream



import asyncio
import time

from rstream import AMQPMessage, Producer, ConfirmationStatus

STREAM = "my-test-stream"
LOOP = 10000
BATCH = 100

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

        start_time = time.perf_counter()

        # sending a million of messages in AMQP format
        for j in range(LOOP):
            messages = []
            for i in range(BATCH):
                amqp_message = AMQPMessage(
                    body=bytes("hello: {}".format(i), "utf-8"),
                )
                messages.append(amqp_message)
            # send_batch is synchronous. will wait till termination
            await producer.send_batch(stream=STREAM, batch=messages, on_publish_confirm=on_publish_confirm)

        end_time = time.perf_counter()
        message_for_second = LOOP * BATCH / (end_time - start_time)
        print(
            f"Sent {LOOP * BATCH} messages in {end_time - start_time:0.4f} seconds. "
            f"{message_for_second:0.4f} messages per second"
        )


asyncio.run(publish())
