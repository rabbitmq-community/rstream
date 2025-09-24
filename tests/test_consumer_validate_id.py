import asyncio
import time
from typing import Optional

import pytest

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    Producer,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
)
from rstream._pyamqp.message import Properties  # type: ignore
from rstream.exceptions import (
    MaxConsumersPerConnectionReached,
)
from tests.util import wait_for

pytestmark = pytest.mark.asyncio


async def test_validate_subscriber_limits():
    try:
        _ = Consumer(
            host="dontcare", username="dontcare", password="dontcare", max_subscribers_by_connection=500
        )
        assert False
    except ValueError:
        assert True

    try:
        _ = Consumer(
            host="dontcare", username="dontcare", password="dontcare", max_subscribers_by_connection=-1
        )
        assert False
    except ValueError:
        assert True


# This test file is created to test this PR https://github.com/rabbitmq-community/rstream/pull/246
# where the _subscribes changed from {reference, Subscriber} to {subscriber_id: Subscriber}
# validate the consumer id.
async def test_validate_subscriber_id_to_stream(stream: str, pytestconfig) -> None:
    consumer = Consumer(
        host=pytestconfig.getoption("rmq_host"),
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        max_subscribers_by_connection=10,
    )
    #  create 10 subscribers and validate the id.
    #  The id should be from 0 to 9 across multiple streams and connections
    for i in range(10):
        sub_id = await consumer.subscribe(stream, lambda x, y: None)
        assert sub_id == i

    assert len(consumer._subscribers) == 10
    # remove one subscriber and validate that we can add a new one with the same id
    await consumer.unsubscribe(5)
    assert 5 not in consumer._subscribers
    sub_id = await consumer.subscribe(stream, lambda x, y: None)
    assert sub_id == 5
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


# test routing to different streams and validate the stream in the callback
async def test_routing_to_stream_(producer: Producer, pytestconfig) -> None:
    # Create streams array
    now = int(time.time())

    def process_data(current_stream: str, msg: AMQPMessage, message_context: MessageContext):
        assert current_stream == message_context.stream
        assert msg.body == bytes("{}".format(current_stream), "utf-8")
        assert msg.body == bytes("{}".format(message_context.stream), "utf-8")

    async def test(current_consumer: Consumer):
        streams = [
            "test_routing_to_stream_0_{}".format(now),
            "test_routing_to_stream_1_{}".format(now),
            "test_routing_to_stream_2_{}".format(now),
        ]

        try:
            for stream in streams:
                await producer.create_stream(stream)

            await current_consumer.subscribe(
                streams[0], lambda msg, ctx: process_data(streams[0], msg, ctx), decoder=amqp_decoder
            )
            await current_consumer.subscribe(
                streams[1], lambda msg, ctx: process_data(streams[1], msg, ctx), decoder=amqp_decoder
            )
            await current_consumer.subscribe(
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
            await wait_for(lambda: len(current_consumer._subscribers) == 3, timeout=5)
        finally:
            for stream in streams:
                await producer.delete_stream(stream)

        await producer.close()
        await current_consumer.close()

    await test(
        Consumer(
            host=pytestconfig.getoption("rmq_host"),
            username=pytestconfig.getoption("rmq_username"),
            password=pytestconfig.getoption("rmq_password"),
            max_subscribers_by_connection=3,
        )
    )


# https://github.com/rabbitmq-community/rstream/issues/235
# validate the subscriber name in the message context when using super stream
# the subscriber name it is not mandatory and the client don't need to create it internally
async def test_validate_subscriber_name_to_super_stream(
    super_stream: str, super_stream_producer: SuperStreamProducer, super_stream_consumer: SuperStreamConsumer
) -> None:
    async def sub(sub_name: Optional[str]):
        def process_data(message_context: MessageContext):
            assert message_context.subscriber_name == sub_name

        await super_stream_consumer.subscribe(
            callback=lambda message, message_context: process_data(message_context=message_context),
            subscriber_name=sub_name,
        )

        for i in range(10):
            amqp_message = AMQPMessage(
                body=bytes("a:{}".format(i), "utf-8"),
                properties=Properties(message_id=str(i)),
            )
            await super_stream_producer.send(amqp_message)
            await asyncio.sleep(0.1)

        await asyncio.sleep(0.5)

    await sub("my-subscriber-name")
    await sub(None)
