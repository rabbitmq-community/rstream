# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
from functools import partial

import pytest

from rstream import (
    AMQPMessage,
    CompressionType,
    Consumer,
    Producer,
    RawMessage,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
    exceptions,
)

from .util import (
    on_publish_confirm_client_callback,
    on_publish_confirm_client_callback2,
    routing_extractor_generic,
    task_to_delete_connection,
    wait_for,
)

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.asyncio


async def test_create_stream_already_exists(stream: str, producer: Producer) -> None:
    with pytest.raises(exceptions.StreamAlreadyExists):
        await producer.create_stream(stream)

    try:
        await producer.create_stream(stream, exists_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_create_super_stream_already_exists(
    super_stream: str, super_stream_producer: SuperStreamProducer
) -> None:
    with pytest.raises(exceptions.StreamAlreadyExists):
        await super_stream_producer.create_super_stream(super_stream, n_partitions=3)

    try:
        await super_stream_producer.create_super_stream(super_stream, n_partitions=3, exists_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_create_and_delete_severalsuper_stream(
    super_stream: str, super_stream_producer: SuperStreamProducer
) -> None:
    await super_stream_producer.create_super_stream("test-super-stream1", n_partitions=3)
    await super_stream_producer.create_super_stream(
        "test-super-stream2", n_partitions=0, binding_keys=["0", "1", "2"]
    )
    await super_stream_producer.delete_super_stream("test-super-stream1")
    await super_stream_producer.create_super_stream("test-super-stream1", n_partitions=3, exists_ok=True)

    await super_stream_producer.create_super_stream("test-super-stream2", n_partitions=3, exists_ok=True)
    await super_stream_producer.delete_super_stream("test-super-stream2")
    await super_stream_producer.delete_super_stream("test-super-stream1")


async def test_delete_stream_doesnt_exist(producer: Producer) -> None:
    with pytest.raises(exceptions.StreamDoesNotExist):
        await producer.delete_stream("not-existing-stream")

    try:
        await producer.delete_stream("not-existing-stream", missing_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_delete_super_stream_doesnt_exist(super_stream_producer: SuperStreamProducer) -> None:
    with pytest.raises(exceptions.StreamDoesNotExist):
        await super_stream_producer.delete_super_stream("not-existing-stream")

    try:
        await super_stream_producer.delete_super_stream("not-existing-stream", missing_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_publishing_sequence(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    assert await producer.send_wait(stream, b"one") == 1
    assert await producer.send_batch(stream, [b"two", b"three"]) == [2, 3]
    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_several_messages(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    for i in range(0, 100000):
        await producer.send(stream, b"one")

    await wait_for(lambda: len(captured) == 100000, 2)


async def test_publishing_several_messages_different_streams(
    stream: str, stream2: str, producer: Producer, consumer: Consumer
) -> None:
    captured_stream_1: list[bytes] = []
    captured_stream_2: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured_stream_1.append(bytes(message))
    )
    await consumer.subscribe(
        stream2, callback=lambda message, message_context: captured_stream_2.append(bytes(message))
    )

    for i in range(0, 100000):
        await producer.send(stream, b"one")
    for i in range(0, 100000):
        await producer.send(stream2, b"one")

    await wait_for(lambda: len(captured_stream_1) == 100000, 2)
    await wait_for(lambda: len(captured_stream_2) == 100000, 2)


async def test_publishing_sequence_subbatching_nocompression(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    list_messages = []
    list_messages.append(b"one")
    list_messages.append(b"two")
    list_messages.append(b"three")

    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.No)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_sequence_subbatching_gzip(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    list_messages = []
    list_messages.append(b"one")
    list_messages.append(b"two")
    list_messages.append(b"three")

    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.Gzip)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publishing_sequence_subbatching_mix(
    stream: str, producer: Producer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    list_messages = []
    list_messages.append(b"one")
    list_messages.append(b"two")
    list_messages.append(b"three")

    await producer.send_batch(stream, list_messages)
    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.Gzip)
    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.No)
    await producer.send_sub_entry(stream, list_messages, compression_type=CompressionType.Gzip)

    await wait_for(lambda: len(captured) == 12)
    assert captured == [
        b"one",
        b"two",
        b"three",
        b"one",
        b"two",
        b"three",
        b"one",
        b"two",
        b"three",
        b"one",
        b"two",
        b"three",
    ]


async def test_publishing_sequence_async(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await producer.send(stream, b"one")
    await producer.send(stream, b"two")
    await producer.send(stream, b"three")

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"one", b"two", b"three"]


async def test_publish_deduplication(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send_wait(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
            )

    await publish_with_ids(1, 2, 3)
    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"test_1", b"test_2", b"test_3"]

    await publish_with_ids(2, 3, 4)

    await wait_for(lambda: len(captured) == 4)
    assert captured == [b"test_1", b"test_2", b"test_3", b"test_4"]


async def test_publish_deduplication_async(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
            )

    await publish_with_ids(1, 2, 3)
    await publish_with_ids(1, 2, 3)

    # give some time to the background thread to publish
    await asyncio.sleep(1)
    await wait_for(lambda: len(captured) == 3)
    assert captured == [b"test_1", b"test_2", b"test_3"]

    await asyncio.sleep(1)
    await publish_with_ids(2, 3, 4)

    await wait_for(lambda: len(captured) == 4)
    assert captured == [b"test_1", b"test_2", b"test_3", b"test_4"]


async def test_concurrent_publish(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await asyncio.gather(
        *(
            producer.send_wait(
                stream,
                RawMessage(b"test", publishing_id),
            )
            for publishing_id in range(1, 11)
        )
    )

    await wait_for(lambda: len(captured) == 10)
    assert captured == [b"test"] * 10


async def test_concurrent_publish_async(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await asyncio.gather(
        *(
            producer.send(
                stream,
                RawMessage(b"test", publishing_id),
            )
            for publishing_id in range(1, 11)
        )
    )

    await wait_for(lambda: len(captured) == 10)
    assert captured == [b"test"] * 10


async def test_send_async_confirmation(stream: str, producer: Producer) -> None:
    confirmed_messages: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback,
                    confirmed_messages=confirmed_messages,
                    errored_messages=errored_messages,
                ),
            )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 3)


# Checks if to different sends can be registered different callbacks
async def test_send_async_confirmation_on_different_callbacks(stream: str, producer: Producer) -> None:
    confirmed_messages: list[int] = []
    confirmed_messages2: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback,
                    confirmed_messages=confirmed_messages,
                    errored_messages=errored_messages,
                ),
            )
            await producer.send(
                stream,
                RawMessage(f"test_{publishing_id}".encode(), publishing_id),
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback2,
                    confirmed_messages=confirmed_messages2,
                    errored_messages=errored_messages,
                ),
            )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 3)
    await wait_for(lambda: len(confirmed_messages2) == 3)


async def test_send_entry_subbatch_async_confirmation(stream: str, producer: Producer) -> None:
    confirmed_messages: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        entry_list = []
        for publishing_id in ids:
            entry_list.append(RawMessage(f"test_{publishing_id}".encode(), publishing_id))

        await producer.send_sub_entry(
            stream,
            entry_list,
            compression_type=CompressionType.Gzip,
            on_publish_confirm=partial(
                on_publish_confirm_client_callback,
                confirmed_messages=confirmed_messages,
                errored_messages=errored_messages,
            ),
        )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 1)


async def test_producer_restart(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await producer.send_wait(stream, b"one")

    await producer.close()
    await producer.start()

    await producer.send_wait(stream, b"two")

    await wait_for(lambda: len(captured) == 2)
    assert captured == [b"one", b"two"]


async def test_publishing_sequence_superstream(
    super_stream: str, super_stream_producer: SuperStreamProducer, super_stream_consumer: SuperStreamConsumer
) -> None:
    captured: list[bytes] = []

    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured.append(bytes(message)), decoder=amqp_decoder
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            amqp_message = AMQPMessage(
                body=bytes("a:{}".format(publishing_id), "utf-8"),
            )

            await super_stream_producer.send(amqp_message)

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)


async def test_publishing_sequence_superstream_key_routing(
    super_stream: str, super_stream_key_routing_producer: SuperStreamProducer, consumer: Consumer
) -> None:
    captured: list[bytes] = []

    await consumer.subscribe(
        stream="test-super-stream-0",
        callback=lambda message, message_context: captured.append(bytes(message)),
        decoder=amqp_decoder,
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            amqp_message = AMQPMessage(body=bytes("a:{}".format(publishing_id), "utf-8"))
            # will send to super_stream with routing key of 'key1'
            await super_stream_key_routing_producer.send(amqp_message)

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)


async def test_publishing_sequence_superstream_binary(
    super_stream: str, super_stream_producer: SuperStreamProducer, super_stream_consumer: SuperStreamConsumer
) -> None:
    captured: list[bytes] = []

    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for _ in ids:
            await super_stream_producer.send(b"one")

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(captured) == 3)


async def test_publishing_sequence_superstream_with_callback(
    super_stream: str, super_stream_producer: SuperStreamProducer
) -> None:
    confirmed_messages: list[int] = []
    errored_messages: list[int] = []

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            amqp_message = AMQPMessage(body=bytes("a:{}".format(publishing_id), "utf-8"))
            await super_stream_producer.send(
                amqp_message,
                on_publish_confirm=partial(
                    on_publish_confirm_client_callback,
                    confirmed_messages=confirmed_messages,
                    errored_messages=errored_messages,
                ),
            )

    await publish_with_ids(1, 2, 3)

    await wait_for(lambda: len(confirmed_messages) == 3)


async def test_producer_connection_broke(stream: str, consumer: Consumer) -> None:
    producer_broke: Producer

    producer_broke = Producer(
        "localhost",
        username="guest",
        password="guest",
        connection_name="test-connection",
    )

    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await producer_broke.start()
    asyncio.create_task(task_to_delete_connection("test-connection"))

    count = 0
    while True:
        await producer_broke.send(stream, b"one")
        count = count + 1
        if count % 100 == 0:
            await asyncio.sleep(0.2)
        if count > 10000:
            break

    await producer_broke.close()

    assert len(captured) == 10001


async def test_super_stream_producer_connection_broke(super_stream: str, consumer: Consumer) -> None:

    super_stream_producer_broke = SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor_generic,
        routing=RouteType.Hash,
        connection_name="test-connection",
        super_stream=super_stream,
    )

    captured_stream1: list[bytes] = []
    captured_stream2: list[bytes] = []
    captured_stream3: list[bytes] = []
    await consumer.subscribe(
        super_stream + "-0", callback=lambda message, message_context: captured_stream1.append(bytes(message))
    )

    await consumer.subscribe(
        super_stream + "-1", callback=lambda message, message_context: captured_stream2.append(bytes(message))
    )

    await consumer.subscribe(
        super_stream + "-2", callback=lambda message, message_context: captured_stream3.append(bytes(message))
    )

    await super_stream_producer_broke.start()

    asyncio.create_task(task_to_delete_connection("test-connection"))
    count = 0
    while True:
        amqp_message = AMQPMessage(
            body=bytes("hello: {}".format(count), "utf-8"),
            application_properties={"id": "{}".format(count)},
        )

        # send is asynchronous
        await super_stream_producer_broke.send(message=amqp_message)
        count = count + 1
        if count % 100 == 0:
            await asyncio.sleep(0.2)
        if count > 10000:
            break

    await super_stream_producer_broke.close()

    assert len(captured_stream1) + len(captured_stream2) + len(captured_stream3)
