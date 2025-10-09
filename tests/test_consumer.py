# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
import time
from functools import partial

import pytest

from rstream import (
    AMQPMessage,
    Consumer,
    ConsumerOffsetSpecification,
    EventContext,
    FilterConfiguration,
    MessageContext,
    OffsetSpecification,
    OffsetType,
    OnClosedErrorInfo,
    Producer,
    Properties,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
    exceptions,
)
from rstream.recovery import BackOffRecoveryStrategy

from .http_requests import http_api_count_connections_by_name
from .util import (
    consumer_update_handler_first,
    consumer_update_handler_next,
    delete_stream_from_consumer,
    http_api_delete_connection_and_check,
    on_message,
    run_consumer,
    wait_for,
)

pytestmark = pytest.mark.asyncio
logger = logging.getLogger(__name__)


async def test_create_stream_already_exists(stream: str, consumer: Consumer) -> None:
    with pytest.raises(exceptions.StreamAlreadyExists):
        await consumer.create_stream(stream)

    try:
        await consumer.create_stream(stream, exists_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_create_super_stream_already_exists(
    super_stream: str, super_stream_consumer: SuperStreamConsumer
) -> None:
    with pytest.raises(exceptions.StreamAlreadyExists):
        await super_stream_consumer.create_super_stream(super_stream, n_partitions=3)

    try:
        await super_stream_consumer.create_super_stream(super_stream, n_partitions=3, exists_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_create_and_delete_severalsuper_stream(
    super_stream: str, super_stream_consumer: SuperStreamConsumer
) -> None:
    await super_stream_consumer.create_super_stream("test-super-stream1", n_partitions=3)
    await super_stream_consumer.create_super_stream(
        "test-super-stream2", n_partitions=0, binding_keys=["0", "1", "2"]
    )
    await super_stream_consumer.delete_super_stream("test-super-stream1")
    await super_stream_consumer.create_super_stream("test-super-stream1", n_partitions=3, exists_ok=True)

    await super_stream_consumer.create_super_stream("test-super-stream2", n_partitions=3, exists_ok=True)
    await super_stream_consumer.delete_super_stream("test-super-stream2")
    await super_stream_consumer.delete_super_stream("test-super-stream1")


async def test_delete_stream_doesnt_exist(consumer: Consumer) -> None:
    with pytest.raises(exceptions.StreamDoesNotExist):
        await consumer.delete_stream("not-existing-stream")

    try:
        await consumer.delete_stream("not-existing-stream", missing_ok=True)
    except Exception:
        pytest.fail("Unexpected error")


async def test_consume(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )
    assert await producer.send_wait(stream, b"one") == 1
    assert await producer.send_batch(stream, [b"two", b"three"]) == [2, 3]

    await wait_for(lambda: len(captured) >= 3)
    assert captured == [b"one", b"two", b"three"]


async def test_offset_type_first(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    captured_offset: list[int] = []

    async def on_message_first(msg: AMQPMessage, message_context: MessageContext):
        captured_offset.append(message_context.offset)
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message_first,
        offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
    )
    messages = [str(i).encode() for i in range(0, 10)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 10)
    assert captured == messages
    assert captured_offset == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


async def test_offset_type_offset(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    captured_offset: list[int] = []

    async def on_message_offset(msg: AMQPMessage, message_context: MessageContext):
        captured_offset.append(message_context.offset)
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message_offset,
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, 7),
    )
    messages = [str(i).encode() for i in range(0, 10)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 3)
    assert captured == messages[7:]
    assert captured_offset == [7, 8, 9]


async def test_offset_type_last(stream: str, consumer: Consumer, producer: Producer) -> None:
    messages = [str(i).encode() for i in range(1, 5_000)]
    await producer.send_batch(stream, messages)

    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.LAST, None),
        subscriber_name="test-subscriber",
    )

    await wait_for(lambda: len(captured) > 0 and captured[-1] == b"4999", 2)
    assert len(captured) < len(messages)


async def test_offset_manual_setting(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.store_offset(stream=stream, offset=7, subscriber_name="test_offset_manual_setting")
    offset = await consumer.query_offset(stream=stream, subscriber_name="test_offset_manual_setting")

    assert offset == 7

    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, offset),
    )

    messages = [str(i).encode() for i in range(1, 11)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 3)


async def test_consumer_callback(stream: str, consumer: Consumer, producer: Producer) -> None:
    streams: list[str] = []
    offsets: list[int] = []

    await consumer.subscribe(
        stream,
        callback=partial(
            on_message,
            streams=streams,
            offsets=offsets,
        ),
        offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
    )

    messages = [str(i).encode() for i in range(0, 10)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(streams) >= 10)
    await wait_for(lambda: len(offsets) >= 10)

    for stream in streams:
        assert "test-stream_" in stream

    for offset in offsets:
        assert offset >= 0 and offset < 100


async def test_consumer_resubscribe_when_not_consumed_events_in_queue(
    consumer: Consumer, producer: Producer
) -> None:
    stream_name = "stream_{}".format(time.time())
    await producer.create_stream(stream=stream_name)

    processed_offsets_1: asyncio.Queue[int] = asyncio.Queue(1)
    processed_offsets_2 = []

    async def long_running_cb(message: AMQPMessage, message_context: MessageContext) -> None:
        await processed_offsets_1.put(message_context.offset)

    async def write_processed_messages_cb(message: AMQPMessage, message_context: MessageContext) -> None:
        processed_offsets_2.append(message_context.offset)

    for _ in range(10):
        await producer.send_wait(
            stream=stream_name,
            message=b"msg",
        )

    try:
        async with consumer:
            subscriber_id = await consumer.subscribe(
                stream=stream_name, callback=long_running_cb, initial_credit=10
            )
            await wait_for(lambda: processed_offsets_1.full())

            await consumer.unsubscribe(subscriber_id)

            await consumer.subscribe(
                stream=stream_name,
                callback=write_processed_messages_cb,
                initial_credit=10,
                offset_specification=ConsumerOffsetSpecification(offset_type=OffsetType.OFFSET, offset=6),
            )

            await wait_for(lambda: len(processed_offsets_2) > 1)
            assert processed_offsets_2[0] == 6
    finally:
        await producer.delete_stream(stream_name)
        await producer.close()


@pytest.mark.flaky(reruns=1, reruns_delay=1)
async def test_offset_type_timestamp(consumer: Consumer, producer: Producer) -> None:
    stream = "test_offset_type_timestamp_{}".format(time.time())
    await producer.create_stream(stream=stream)
    messages = [str(i).encode() for i in range(1, 5_000)]
    await producer.send_batch(stream, messages)

    # mark time in between message batches
    await asyncio.sleep(1)
    now = int(time.time() * 1000)

    messages = [str(i).encode() for i in range(5_000, 5_100)]
    await producer.send_batch(stream, messages)

    captured: list[bytes] = []

    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(offset_type=OffsetType.TIMESTAMP, offset=now),
    )
    await wait_for(lambda: len(captured) > 10, 5, 2)
    assert captured[0] >= b"5000"
    await producer.delete_stream(stream)


async def test_offset_type_next(stream: str, consumer: Consumer, producer: Producer) -> None:
    messages = [str(i).encode() for i in range(1, 11)]
    await producer.send_batch(stream, messages)

    captured: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
        subscriber_name="test-subscriber",
    )
    await producer.send_wait(stream, b"11")
    await wait_for(lambda: len(captured) > 0)
    assert captured == [b"11"]


async def test_consume_with_resubscribe(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured_by_first_consumer: list[bytes] = []
    subscriber_id = await consumer.subscribe(
        stream, callback=lambda message, message_context: captured_by_first_consumer.append(bytes(message))
    )
    await producer.send_wait(stream, b"one")
    await wait_for(lambda: len(captured_by_first_consumer) >= 1)
    assert captured_by_first_consumer == [b"one"]

    await consumer.unsubscribe(subscriber_id)

    captured_by_second_consumer: list[bytes] = []
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured_by_second_consumer.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    await producer.send_wait(stream, b"two")
    await asyncio.sleep(1)
    await wait_for(lambda: len(captured_by_second_consumer) >= 1)
    assert captured_by_second_consumer == [b"two"]


async def test_consume_with_resubscribe_msg(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured_by_first_consumer: list[bytes] = []
    subscriber_name = "my-subscriber"
    subscriber_id = await consumer.subscribe(
        stream,
        subscriber_name=subscriber_name,
        callback=lambda message, message_context: captured_by_first_consumer.append(bytes(message)),
    )
    for i in range(100):
        await producer.send_wait(stream, b"one")
    await wait_for(lambda: len(captured_by_first_consumer) >= 100)

    await consumer.unsubscribe(subscriber_id)

    captured_by_second_consumer: list[bytes] = []
    await consumer.subscribe(
        stream,
        subscriber_name=subscriber_name,
        callback=lambda message, message_context: captured_by_second_consumer.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    for i in range(100):
        await producer.send_wait(stream, b"two")
    await wait_for(lambda: len(captured_by_second_consumer) >= 100)


async def test_consume_superstream_with_resubscribe(
    super_stream: str, super_stream_consumer: SuperStreamConsumer, super_stream_producer: SuperStreamProducer
) -> None:
    captured_by_first_consumer: list[bytes] = []
    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured_by_first_consumer.append(bytes(message))
    )
    await super_stream_producer.send(b"one")
    await wait_for(lambda: len(captured_by_first_consumer) >= 1)

    await super_stream_consumer.unsubscribe()

    captured_by_second_consumer: list[bytes] = []
    await super_stream_consumer.subscribe(
        callback=lambda message, message_context: captured_by_second_consumer.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    await super_stream_producer.send(b"two")

    await wait_for(lambda: len(captured_by_second_consumer) >= 1)
    assert captured_by_second_consumer == [b"two"]


async def test_consume_with_restart(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )
    await producer.send_wait(stream, b"one")
    await wait_for(lambda: len(captured) >= 1)

    await consumer.close()
    await consumer.start()
    await consumer.subscribe(
        stream,
        callback=lambda message, message_context: captured.append(bytes(message)),
        offset_specification=ConsumerOffsetSpecification(OffsetType.NEXT, None),
    )

    await producer.send_wait(stream, b"two")
    await wait_for(lambda: len(captured) >= 2)
    assert captured == [b"one", b"two"]


async def test_consume_multiple_streams(consumer: Consumer, producer: Producer) -> None:
    streams = ["stream1", "stream2", "stream3"]
    try:
        await asyncio.gather(*(consumer.create_stream(stream) for stream in streams))

        captured: list[bytes] = []
        await asyncio.gather(
            *(
                consumer.subscribe(
                    stream, callback=lambda message, message_context: captured.append(bytes(message))
                )
                for stream in streams
            )
        )

        await asyncio.gather(*(producer.send_wait(stream, b"test") for stream in streams))

        await wait_for(lambda: len(captured) >= 3)
        assert captured == [b"test", b"test", b"test"]

    finally:
        await producer.close()
        await asyncio.gather(*(consumer.delete_stream(stream) for stream in streams))


async def test_consume_with_sac_custom_consumer_update_listener_cb(
    consumer: Consumer, producer: Producer
) -> None:
    stream_name = "stream_test_consume_with_sac_custom_consumer_update_listener_cb_{}".format(time.time())
    await producer.create_stream(stream=stream_name)
    try:
        # necessary to use send_batch, since in this case, upon delivery, rabbitmq will deliver
        # this batch as a whole, and not one message at a time, like send_wait
        await producer.send_batch(stream_name, [AMQPMessage(body=f"{i}".encode()) for i in range(10)])

        received_offsets = []

        async def consumer_cb(message: bytes, message_context: MessageContext) -> None:
            received_offsets.append(message_context.offset)

        async def consumer_update_listener_with_custom_offset(
            is_active: bool, event_context: EventContext
        ) -> OffsetSpecification:
            if is_active:
                return OffsetSpecification(offset_type=OffsetType.OFFSET, offset=5)
            return OffsetSpecification(offset_type=OffsetType.FIRST, offset=0)

        properties = {"single-active-consumer": "true", "name": "sac_name"}
        async with consumer:
            await consumer.subscribe(
                stream=stream_name,
                callback=consumer_cb,
                properties=properties,
                offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST),
                consumer_update_listener=consumer_update_listener_with_custom_offset,
            )

            await wait_for(lambda: len(received_offsets) >= 1)

            assert received_offsets[0] == 5

    finally:
        await producer.delete_stream(stream=stream_name)
        await producer.close()


async def test_consume_with_multiple_sac_custom_consumer_update_listener_cb(
    consumer: Consumer, producer: Producer
) -> None:
    stream_name_1 = "test_consume_with_multiple_sac_custom_consumer_update_listener_cb_1{}".format(
        time.time()
    )
    stream_name_2 = "test_consume_with_multiple_sac_custom_consumer_update_listener_cb_2{}".format(
        time.time()
    )
    await producer.create_stream(stream=stream_name_1)
    await producer.create_stream(stream=stream_name_2)
    try:
        # necessary to use send_wait here, because rmq will store every message in separate batch.
        # In case of use send_batch rstream will filter messages on the client side bypassing some problems.
        for i in range(10):
            await producer.send_wait(stream_name_1, AMQPMessage(body=f"{i}".encode()))
            await producer.send_wait(stream_name_2, AMQPMessage(body=f"{i}".encode()))

        received_offsets_1 = []
        received_offsets_2 = []

        async def consumer_cb1(message: AMQPMessage, message_context: MessageContext) -> None:
            received_offsets_1.append(message_context.offset)

        async def consumer_cb2(message: AMQPMessage, message_context: MessageContext) -> None:
            received_offsets_2.append(message_context.offset)

        async def consumer_update_listener_with_custom_offset_1(
            is_active: bool, event_context: EventContext
        ) -> OffsetSpecification:
            if is_active:
                return OffsetSpecification(offset_type=OffsetType.OFFSET, offset=5)
            return OffsetSpecification(offset_type=OffsetType.FIRST, offset=0)

        async def consumer_update_listener_with_custom_offset_2(
            is_active: bool, event_context: EventContext
        ) -> OffsetSpecification:
            if is_active:
                return OffsetSpecification(offset_type=OffsetType.OFFSET, offset=7)
            return OffsetSpecification(offset_type=OffsetType.FIRST, offset=0)

        async with consumer:
            await consumer.subscribe(
                stream=stream_name_1,
                callback=consumer_cb1,
                properties={"single-active-consumer": "true", "name": "sac_name1"},
                offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST),
                consumer_update_listener=consumer_update_listener_with_custom_offset_1,
            )
            await consumer.subscribe(
                stream=stream_name_2,
                callback=consumer_cb2,
                properties={"single-active-consumer": "true", "name": "sac_name2"},
                offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST),
                consumer_update_listener=consumer_update_listener_with_custom_offset_2,
            )

            await wait_for(lambda: len(received_offsets_1) >= 1)
            await wait_for(lambda: len(received_offsets_2) >= 1)

            assert received_offsets_1[0] == 5
            assert received_offsets_2[0] == 7

    finally:
        await producer.delete_stream(stream=stream_name_1)
        await producer.delete_stream(stream=stream_name_2)
        await producer.close()


async def test_consume_superstream_with_sac_all_active(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body=bytes("a:{}".format(i), "utf-8"),
            properties=Properties(message_id=str(i)),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_consume_superstream_with_sac_one_non_active(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_consumer_for_sac4: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []
    consumer_stream_list4: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3)
    await run_consumer(super_stream_consumer_for_sac4, consumer_stream_list4)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body=bytes("a:{}".format(i), "utf-8"),
            properties=Properties(message_id=str(i)),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)
    consumer_stream_set4 = set(consumer_stream_list4)

    assert (
        len(consumer_stream_set1) == 0
        or len(consumer_stream_set2) == 0
        or len(consumer_stream_set3) == 0
        or len(consumer_stream_set4) == 0
    )

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)
    consumers_set = consumers_set.union(consumer_stream_list4)

    # one consumer was alway inactive
    assert len(consumers_set) == 3


async def test_consume_superstream_with_callback_next(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1, consumer_update_handler_next)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2, consumer_update_handler_next)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3, consumer_update_handler_next)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body=bytes("a:{}".format(i), "utf-8"),
            properties=Properties(message_id=str(i)),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_consume_superstream_with_callback_first(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1, consumer_update_handler_first)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2, consumer_update_handler_first)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3, consumer_update_handler_first)

    for i in range(10000):
        amqp_message = AMQPMessage(
            body=bytes("a:{}".format(i), "utf-8"),
            properties=Properties(message_id=str(i)),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_consume_superstream_with_callback_offset(
    super_stream: str,
    super_stream_consumer_for_sac1: SuperStreamConsumer,
    super_stream_consumer_for_sac2: SuperStreamConsumer,
    super_stream_consumer_for_sac3: SuperStreamConsumer,
    super_stream_producer_for_sac: SuperStreamProducer,
) -> None:
    consumer_stream_list1: list[str] = []
    consumer_stream_list2: list[str] = []
    consumer_stream_list3: list[str] = []

    await run_consumer(super_stream_consumer_for_sac1, consumer_stream_list1, consumer_update_handler_first)
    await run_consumer(super_stream_consumer_for_sac2, consumer_stream_list2, consumer_update_handler_first)
    await run_consumer(super_stream_consumer_for_sac3, consumer_stream_list3, consumer_update_handler_first)

    for i in range(10_000):
        amqp_message = AMQPMessage(
            body=bytes("a:{}".format(i), "utf-8"),
            properties=Properties(message_id=str(i)),
        )
        await super_stream_producer_for_sac.send(amqp_message)

    await asyncio.sleep(1)

    # check that the total number of messages have been consumed by the consumers
    assert len(consumer_stream_list1) + len(consumer_stream_list2) + len(consumer_stream_list3) == 10000

    consumer_stream_set1 = set(consumer_stream_list1)
    consumer_stream_set2 = set(consumer_stream_list2)
    consumer_stream_set3 = set(consumer_stream_list3)

    # check that every consumer consumed in just one set
    assert len(consumer_stream_set1) == 1
    assert len(consumer_stream_set2) == 1
    assert len(consumer_stream_set3) == 1

    consumers_set = consumer_stream_set1.union(consumer_stream_set2)
    consumers_set = consumers_set.union(consumer_stream_list3)

    assert len(consumers_set) == 3


async def test_callback_sync_request(stream: str, consumer: Consumer, producer: Producer) -> None:
    captured: list[bytes] = []

    async def on_message_first(msg: AMQPMessage, message_context: MessageContext):
        captured.append(bytes(msg))
        await consumer.close()

    await consumer.subscribe(stream, callback=on_message_first)
    messages = [str(i).encode() for i in range(0, 1)]
    await producer.send_batch(stream, messages)

    await wait_for(lambda: len(captured) >= 1)


async def test_consumer_connection_broke_with_recovery_disabled(stream: str) -> None:
    connection_broke = False
    stream_disconnected = None
    consumer_broke: Consumer
    conn_name = "test_consumer_connection_broke_{}".format(time.time())

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

        await consumer_broke.close()

    consumer_broke = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name=conn_name,
        recovery_strategy=BackOffRecoveryStrategy(False),
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        pass

    await consumer_broke.start()
    await consumer_broke.subscribe(stream=stream, callback=on_message, decoder=amqp_decoder)

    asyncio.create_task(consumer_broke.run())
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 10)
    await http_api_delete_connection_and_check(conn_name)
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 0, 10)

    await wait_for(lambda: connection_broke, 10)
    await wait_for(lambda: stream_disconnected == stream, 10)
    await consumer_broke.close()


async def test_super_stream_consumer_connection_broke_with_recovery_disabled(super_stream: str) -> None:
    connection_broke = False
    streams_disconnected: set[str] = set()
    conn_name = "test_super_stream_consumer_connection_broke_{}".format(time.time())

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal connection_broke
        # avoiding multiple connection closed to hit

        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)

        connection_broke = True
        await super_stream_consumer_broke.close()
        return None

    super_stream_consumer_broke = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name=conn_name,
        super_stream=super_stream,
        recovery_strategy=BackOffRecoveryStrategy(False),
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        pass

    await super_stream_consumer_broke.start()
    await super_stream_consumer_broke.subscribe(callback=on_message, decoder=amqp_decoder)
    asyncio.create_task(super_stream_consumer_broke.run())
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 5)
    await http_api_delete_connection_and_check(conn_name)
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 0, 5)

    assert connection_broke is True
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected


# Send a few messages to a superstream, consume, simulate a disconnection and check for reconnection
async def test_super_stream_consumer_connection_broke_recovery_disabled(super_stream: str) -> None:
    connection_broke = False
    streams_disconnected: set[str] = set()
    conn_name = "test_super_stream_consumer_connection_broke_recovery_disabled_{}".format(time.time())

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        logger.warning("connection closed")
        nonlocal connection_broke
        # avoiding multiple connection closed to hit
        if connection_broke:
            for stream in disconnection_info.streams:
                streams_disconnected.add(stream)
            return None

        connection_broke = True

        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)

        await super_stream_consumer_broke.close()
        return None

    super_stream_consumer_broke = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name=conn_name,
        super_stream=super_stream,
        recovery_strategy=BackOffRecoveryStrategy(False),
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        pass

    await super_stream_consumer_broke.start()
    await super_stream_consumer_broke.subscribe(callback=on_message, decoder=amqp_decoder)

    asyncio.create_task(super_stream_consumer_broke.run())
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 5)
    await http_api_delete_connection_and_check(conn_name)
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 0, 5)
    await wait_for(lambda: connection_broke, 10)
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected


async def test_consume_filtering(stream: str, consumer: Consumer, producer_with_filtering: Producer) -> None:
    filters = ["1"]

    captured: list[bytes] = []

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message,
        decoder=amqp_decoder,
        filter_input=FilterConfiguration(
            values_to_filter=filters,
            predicate=lambda message: message.application_properties[b"id"] == filters[0].encode("utf-8"),
            match_unfiltered=False,
        ),
    )

    for j in range(10):
        messages = []
        for i in range(50):
            application_properties = {
                "id": str(i),
            }
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                application_properties=application_properties,
            )
            messages.append(amqp_message)
        # send_batch is synchronous. will wait till termination
        await producer_with_filtering.send_batch(stream=stream, batch=messages)  # type: ignore

    # Consumed just the filetered items
    await wait_for(lambda: len(captured) == 10)


async def test_consume_filtering_match_unfiltered(
    stream: str, consumer: Consumer, producer: Producer
) -> None:
    filters = ["1"]

    captured: list[bytes] = []

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        captured.append(bytes(msg))

    await consumer.subscribe(
        stream,
        callback=on_message,
        decoder=amqp_decoder,
        filter_input=FilterConfiguration(
            values_to_filter=filters,
            predicate=lambda message: message.application_properties[b"id"] == filters[0].encode("utf-8"),
            match_unfiltered=False,
        ),
    )

    for j in range(10):
        messages = []
        for i in range(50):
            application_properties = {
                "id": str(i),
            }
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                application_properties=application_properties,
            )
            messages.append(amqp_message)
        # send_batch is synchronous. will wait till termination
        await producer.send_batch(stream=stream, batch=messages)  # type: ignore

    # No filter on produce side no filetering
    await wait_for(lambda: len(captured) == 0)


async def test_consumer_metadata_update(consumer: Consumer) -> None:
    consumer_closed = False
    stream_disconnected = None
    stream = "test-stream-metadata-update"
    consumer_metadata_update: Consumer

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal consumer_closed

        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

        if consumer_closed is False:
            consumer_closed = True
            await consumer_metadata_update.close()

    consumer_metadata_update = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name="test-connection",
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        pass

    await consumer_metadata_update.start()
    await consumer_metadata_update.create_stream(stream)
    asyncio.create_task(delete_stream_from_consumer(consumer, stream))
    await consumer_metadata_update.subscribe(stream=stream, callback=on_message, decoder=amqp_decoder)
    await consumer_metadata_update.run()

    assert consumer_closed is True
    assert stream_disconnected == stream

    await asyncio.sleep(1)


# test the consumer reconnection when the connection is closed by the HTTP
# API and the recovery is enabled in the consumer
# test validate also the offset is preserved and no messages are lost, the consumer
# should restart from the last offset stored
async def test_consumer_connection_broke_with_recovery_enabled(stream: str, producer: Producer) -> None:
    connection_broke = False
    stream_disconnected = None
    consumer_recovery: Consumer
    conn_name = "test_consumer_connection_broke_with_recovery_enabled_{}".format(time.time())
    amqp_message = AMQPMessage(
        body=bytes("first", "utf-8"),
    )
    await producer.send_wait(stream=stream, message=amqp_message)

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        nonlocal connection_broke
        connection_broke = True
        nonlocal stream_disconnected
        stream_disconnected = disconnection_info.streams.pop()

    consumer_recovery = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name=conn_name,
        recovery_strategy=BackOffRecoveryStrategy(True, jitter=1),
    )

    offset_received = []

    async def on_message_c(msg: AMQPMessage, message_context: MessageContext):
        offset_received.append(message_context.offset)
        pass

    await consumer_recovery.start()
    await consumer_recovery.subscribe(stream=stream, callback=on_message_c, decoder=amqp_decoder)

    asyncio.create_task(consumer_recovery.run())
    await wait_for(lambda: offset_received == [0], 5)
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 10)
    await http_api_delete_connection_and_check(conn_name)
    amqp_message = AMQPMessage(
        body=bytes("second", "utf-8"),
    )
    await producer.send_wait(stream=stream, message=amqp_message)
    await wait_for(lambda: connection_broke, 1)
    await wait_for(lambda: stream_disconnected == stream, 10)
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 10)
    await wait_for(lambda: offset_received == [0, 1], 10)
    await consumer_recovery.close()
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 0, 10)


# test the super stream consumer reconnection when the connection is closed by the
# HTTP API and the recovery is enabled in the super stream consumer
async def test_super_stream_consumer_connection_broke_with_recovery_enabled(
    super_stream: str, super_stream_producer: SuperStreamProducer
) -> None:
    streams_disconnected: set[str] = set()
    conn_name = "test_super_stream_consumer_connection_broke_with_recovery_enabled_{}".format(time.time())
    await super_stream_producer.send(b"one")

    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        for stream in disconnection_info.streams:
            streams_disconnected.add(stream)

    super_stream_consumer_recovery = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        on_close_handler=on_connection_closed,
        connection_name=conn_name,
        super_stream=super_stream,
        recovery_strategy=BackOffRecoveryStrategy(True, jitter=1),
    )
    offset_received = []

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        offset_received.append(message_context.offset)
        pass

    await super_stream_consumer_recovery.start()
    await super_stream_consumer_recovery.subscribe(
        callback=on_message, offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None)
    )

    asyncio.create_task(super_stream_consumer_recovery.run())
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 10)
    await http_api_delete_connection_and_check(conn_name)
    await super_stream_producer.send(b"two")
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 10)
    assert "test-super-stream-0" in streams_disconnected
    assert "test-super-stream-1" in streams_disconnected
    assert "test-super-stream-2" in streams_disconnected
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 1, 10)
    await wait_for(lambda: offset_received == [0, 1], 10)
    await super_stream_consumer_recovery.close()
    await wait_for(lambda: http_api_count_connections_by_name(conn_name) == 0, 10)
