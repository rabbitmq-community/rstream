import asyncio
import logging
import os
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
from rstream.exceptions import StreamAlreadySubscribed
from tests.http_requests import count_connections_by_name
from tests.util import wait_for

pytestmark = pytest.mark.asyncio

# This test file is created to test this PR https://github.com/rabbitmq-community/rstream/pull/246
# the scope it to validate the news implementations.
# the cluster version is disabled in the CI actions, but you can enable it locally to validate the tests.
# to run the tests in cluster mode you need to have a rabbitmq cluster with 3 nodes.
# execute the tests with the following command:
# make rabbitmq-ha-proxy
# IN_GITHUB_ACTIONS is used to skip the clustering tests in the CI actions

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

LOGGER = logging.getLogger(__name__)


async def test_validate_subscriber_limits():
    LOGGER.info("Validating subscriber limits")
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


# avoid to subscribe two times the same stream
async def test_avoid_two_times_subscriber_limits(stream: str, consumer: Consumer):
    await consumer.subscribe(stream, lambda x, y: None)
    try:
        await consumer.subscribe(stream, lambda x, y: None)
        assert False
    except StreamAlreadySubscribed:
        assert True


# the test is to validate the recycle_subscriber_id
# the is always incremented and never recycled until reach the MAX_ITEM_ALLOWED
# after that the id should be recycled.
# the client does not recycle immediately to avoid race conditions during the reuse of the id.
# the id is only a value and the user should not care about the sequence of it
async def test_validate_recycle_subscriber_id(stream: str, consumer: Consumer) -> None:
    for i in range(200):  # MAX_ITEM_ALLOWED is 200
        sub_id = await consumer.subscribe(stream, lambda x, y: None)
        assert sub_id == i
        # even there are free slots, the id should not be recycled
        await consumer.unsubscribe(sub_id)
        assert sub_id not in consumer._subscribers
        assert len(consumer._subscribers) == 0

    # here we reach the max ID == MAX_ITEM_ALLOWED
    # next subscribe should be 0 the first free slot
    sub_id = await consumer.subscribe(stream, lambda x, y: None)
    assert sub_id == 0
    await consumer.unsubscribe(sub_id)


# where the _subscribes changed from {reference, Subscriber} to {subscriber_id: Subscriber}
# validate the consumer id.
async def test_validate_subscriber_id_to_stream(consumer: Consumer) -> None:
    LOGGER.info("Validating subscriber id to stream")
    consumer._max_subscribers_by_connection = 2
    now = int(time.time())
    streams = [
        "test_validate_subscriber_id_to_stream_0_{}".format(now),
        "test_validate_subscriber_id_to_stream_1_{}".format(now),
        "test_validate_subscriber_id_to_stream_2_{}".format(now),
    ]

    for stream in streams:
        await consumer.create_stream(stream)

    #  create 3 subscribers and validate the id.
    for i, stream in enumerate(streams):
        sub_id = await consumer.subscribe(stream, lambda x, y: None)
        assert sub_id == i

    assert len(consumer._subscribers) == 3
    assert len(consumer._clients) == 3
    # remove one subscriber and validate that we can add a new one with the same id
    await consumer.unsubscribe(0)
    assert 0 not in consumer._subscribers
    sub_id = await consumer.subscribe(streams[0], lambda x, y: None)
    assert sub_id == 3
    assert len(consumer._subscribers) == 3
    newStream = "test_validate_subscriber_id_to_stream_3_{}".format(now)
    await consumer.create_stream(newStream)
    # add a new subscriber and validate that a new connection is created
    sub_id = await consumer.subscribe(newStream, lambda x, y: None)
    assert sub_id == 4
    assert len(consumer._subscribers) == 4
    assert len(consumer._clients) == 4
    sequence = [1, 2, 3, 4]
    for i in sequence:
        await consumer.unsubscribe(i)
        assert i not in consumer._subscribers

    await consumer.close()
    assert len(consumer._subscribers) == 0
    for stream in streams:
        await consumer.delete_stream(stream)

    await consumer.delete_stream(newStream)

    await consumer.close()
    assert len(consumer._subscribers) == 0


# test routing to different streams and validate the stream in the callback
async def test_routing_to_stream_(producer: Producer, pytestconfig) -> None:
    LOGGER.info("Testing routing to stream")
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
    LOGGER.info("Validating subscriber name to super stream")

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
        await super_stream_consumer.close()

    await sub("my-subscriber-name")
    await sub(None)


async def test_validate_publisher_id_to_stream(producer: Producer, pytestconfig) -> None:
    LOGGER.info("Validating publisher id to stream")
    now = int(time.time())
    streams = [
        "test_test_validate_publisher_id_to_stream_0_{}".format(now),
        "test_test_validate_publisher_id_to_stream_1_{}".format(now),
        "test_test_validate_publisher_id_to_stream_2_{}".format(now),
    ]

    for stream in streams:
        await producer.create_stream(stream)

    for stream in streams:
        for i in range(2):
            await producer.send_wait(stream, AMQPMessage(body=bytes("hello: {}".format(i), "utf-8")))

    await asyncio.sleep(1)
    assert len(producer._publishers) == 3
    for _publisher in producer._publishers.values():
        assert _publisher.id in [0, 1, 2]

    await producer.close()
    assert len(producer._publishers) == 0
    for stream in streams:
        await producer.delete_stream(stream)


# cluster test
#  skip if not in github actions
@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skip cluster tests in GitHub Actions")
async def test_validate_publisher_id_to_stream_cluster(cluster_producer: Producer) -> None:
    LOGGER.info("Validating publisher id to stream cluster")
    # simple test to validate the publisher id in cluster mode
    cluster_producer._max_publishers_by_connection = 3
    now = int(time.time())
    streams = [
        "test_validate_publisher_id_to_stream_cluster_0_{}".format(now),
        "test_validate_publisher_id_to_stream_cluster_1_{}".format(now),
        "test_validate_publisher_id_to_stream_cluster_2_{}".format(now),
    ]
    for stream in streams:
        await cluster_producer.create_stream(stream)

    for stream in streams:
        for i in range(2):
            await cluster_producer.send_wait(stream, AMQPMessage(body=bytes("hello: {}".format(i), "utf-8")))
    await asyncio.sleep(0.500)

    assert len(cluster_producer._publishers) == 3
    for _publisher in cluster_producer._publishers.values():
        assert _publisher.id in [0, 1, 2]

    for stream in streams:
        await cluster_producer.delete_stream(stream)

    await cluster_producer.close()
    assert len(cluster_producer._publishers) == 0


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skip cluster tests in GitHub Actions")
async def test_spin_new_connection_for_producer_stream_cluster(
    cluster_producer: Producer, http_cluster_port: int
) -> None:
    LOGGER.info("Validating publisher id to stream cluster")
    # test to validate if the producer spin up a new connection when the max publishers per connection is reached
    cluster_producer._max_publishers_by_connection = 1
    now = int(time.time())
    conn_name = "test_spin_new_connection__stream_cluster_{}".format(now)
    cluster_producer._connection_name = conn_name
    streams = [
        "test_spin_new_connection__stream_cluster_0_{}".format(now),
        "test_spin_new_connection__stream_cluster_1_{}".format(now),
        "test_spin_new_connection__stream_cluster_2_{}".format(now),
        "test_spin_new_connection__stream_cluster_3_{}".format(now),
    ]
    for stream in streams:
        await cluster_producer.create_stream(stream)

    for stream in streams:
        for i in range(2):
            await cluster_producer.send_wait(stream, AMQPMessage(body=bytes("hello: {}".format(i), "utf-8")))
    await asyncio.sleep(0.500)

    assert len(cluster_producer._publishers) == 4
    for _publisher in cluster_producer._publishers.values():
        assert _publisher.id in [0, 1, 2, 3]
    assert len(cluster_producer._clients) == 4

    await wait_for(lambda: count_connections_by_name(conn_name, http_cluster_port) == 3, timeout=10)

    await cluster_producer.close()
    assert len(cluster_producer._publishers) == 0
    assert len(cluster_producer._clients) == 0

    for stream in streams:
        await cluster_producer.delete_stream(stream)


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Skip cluster tests in GitHub Actions")
async def test_spin_new_connection_for_consumer_stream_cluster(
    cluster_consumer: Consumer, http_cluster_port: int
) -> None:
    LOGGER.info("Validating spin new connection for consumer stream cluster")
    # test to validate if the consumer spin up a new connection when the max subscribers per connection is reached
    now = int(time.time())
    conn_name = "test_spin_new_connection_for_consumer_stream_cluster_{}".format(now)
    cluster_consumer._connection_name = conn_name
    streams = [
        "test_spin_new_connection_for_consumer_stream_cluster_0_{}".format(now),
        "test_spin_new_connection_for_consumer_stream_cluster_1_{}".format(now),
        "test_spin_new_connection_for_consumer_stream_cluster_2_{}".format(now),
        "test_spin_new_connection_for_consumer_stream_cluster_3_{}".format(now),
    ]
    for stream in streams:
        await cluster_consumer.create_stream(stream)

    for stream in streams:
        await cluster_consumer.subscribe(stream, lambda x, y: None)

    await asyncio.sleep(0.500)

    assert len(cluster_consumer._subscribers) == 4
    for subscriber_id in cluster_consumer._subscribers.keys():
        assert subscriber_id in [0, 1, 2, 3]
    assert len(cluster_consumer._clients) == 4

    await wait_for(lambda: count_connections_by_name(conn_name, http_cluster_port) > 0, timeout=10)

    await cluster_consumer.close()
    assert len(cluster_consumer._subscribers) == 0
    assert len(cluster_consumer._clients) == 0
    await wait_for(lambda: count_connections_by_name(conn_name, http_cluster_port) == 0, timeout=10)

    for stream in streams:
        await cluster_consumer.delete_stream(stream)
