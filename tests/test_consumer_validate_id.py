import asyncio
import time

import pytest

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    Producer,
    amqp_decoder,
)
from rstream.exceptions import (
    MaxConsumersPerConnectionReached,
)
from tests.util import wait_for

pytestmark = pytest.mark.asyncio


# This test file is created to test this PR https://github.com/rabbitmq-community/rstream/pull/246
# where the _subscribes changed from {reference, Subscriber} to {subscriber_id: Subscriber}


# validate the consumer id.
async def test_validate_subscriber_id(stream: str) -> None:
    consumer = Consumer(
        host="localhost", username="guest", password="guest", max_subscribers_by_connection=10
    )
    #  create 10 subscribers and validate the id.
    #  The id should be from 0 to 9 across multiple streams and connections
    for i in range(10):
        sub_id = await consumer.subscribe(stream, lambda x, y: None)
        assert sub_id == i

    assert len(consumer._subscribers) == 10

    try:
        await consumer.subscribe(stream, lambda x, y: None)
        assert False
    except MaxConsumersPerConnectionReached:
        assert True

    for i in range(10):
        await consumer.unsubscribe(i)
        assert i not in consumer._subscribers

    await consumer.close()
    assert len(consumer._subscribers) == 0


async def test_routing_to_stream_(producer: Producer, consumer: Consumer) -> None:
    # Create streams array
    now = int(time.time())

    def process_data(current_stream: str, msg: AMQPMessage, message_context: MessageContext):
        assert current_stream == message_context.stream
        assert msg.body == bytes("{}".format(current_stream), "utf-8")
        assert msg.body == bytes("{}".format(message_context.stream), "utf-8")

    streams = [
        "test_routing_to_stream_0_{}".format(now),
        "test_routing_to_stream_1_{}".format(now),
        "test_routing_to_stream_2_{}".format(now),
    ]

    try:
        for stream in streams:
            await producer.create_stream(stream)

        await consumer.subscribe(
            streams[0], lambda msg, ctx: process_data(streams[0], msg, ctx), decoder=amqp_decoder
        )
        await consumer.subscribe(
            streams[1], lambda msg, ctx: process_data(streams[1], msg, ctx), decoder=amqp_decoder
        )
        await consumer.subscribe(
            streams[2], lambda msg, ctx: process_data(streams[2], msg, ctx), decoder=amqp_decoder
        )

        for i in range(12):
            amqp_message = AMQPMessage(
                body=bytes("{}".format(streams[i % 3]), "utf-8"),
            )
            stream_to_send = streams[i % 3]
            await producer.send_wait(stream_to_send, amqp_message)
            await asyncio.sleep(0.1)

        await asyncio.sleep(0.5)
        await wait_for(lambda: len(consumer._subscribers) == 3, timeout=5)
    finally:
        for stream in streams:
            await producer.delete_stream(stream)

    await producer.close()
    await consumer.close()
