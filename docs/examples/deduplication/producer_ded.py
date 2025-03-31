import asyncio
import time

from rstream import AMQPMessage, Producer, RawMessage

STREAM = "my-test-stream"
LOOP_1 = 1000
LOOP_2 = 1000


async def publish():
    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for j in range(LOOP_1):
            for i in range(LOOP_2):
                amqp_message = AMQPMessage(
                    body=bytes("hello: {}".format(i), "utf-8"),
                )
                amqp_message.publishing_id = j
                await producer.send(
                    stream=STREAM,
                    publisher_name="publisher_name",
                    # just 1000 messages will be inserted as messages with the same publishing_id and publisher_name will be discarded
                    message=amqp_message,
                )

        end_time = time.perf_counter()
        print(f"Sent {LOOP_1} messages in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
