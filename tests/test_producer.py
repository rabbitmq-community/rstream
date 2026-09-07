# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio
import logging
import time
from functools import partial
from types import SimpleNamespace

import pytest

from rstream import (
    AMQPMessage,
    CompressionType,
    ConfirmationStatus,
    Consumer,
    OnClosedErrorInfo,
    Producer,
    RawMessage,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
    amqp_decoder,
    exceptions,
    schema,
    utils,
)
from rstream.client import Client
from rstream.encoding import encode_publish
from rstream.producer import _MessageNotification, _Publisher

from .util import (
    http_api_delete_connection_and_check,
    on_publish_confirm_client_callback,
    on_publish_confirm_client_callback2,
    routing_extractor_generic,
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


async def test_create_and_delete_several_super_stream(
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


async def test_stream_exists_does_not_accumulate_locator_clients(producer: Producer) -> None:
    """Regression: Producer.stream_exists() opens and immediately closes a short-lived
    locator connection on every call. Those closed clients must be evicted from
    ClientPool instead of accumulating (one leaked Client per call) for the lifetime
    of the producer."""
    stream_name = "test-stream-locator-pool-{}".format(time.time())
    for _ in range(10):
        # a stream that does not exist makes stream_exists() return False, so the
        # producer never sends and no partition/long-lived client is created
        assert await producer.stream_exists(stream_name) is False

    total_clients = sum(len(clients) for clients in producer._pool._clients.values())
    assert total_clients == 1


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


async def test_send_splits_batch_exceeding_frame_max(stream: str, consumer: Consumer) -> None:
    # With a small frame_max, a buffered batch whose messages would together
    # exceed it must be split into several Publish frames instead of being
    # sent (and rejected/dropped by the broker) as a single oversized frame.
    frame_max = 5000
    message_size = 1000
    num_messages = 40
    payload = b"x" * message_size

    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    producer = Producer("localhost", username="guest", password="guest", frame_max=frame_max)
    await producer.start()
    try:
        for _ in range(num_messages):
            await producer.send(stream, payload)

        await wait_for(lambda: len(captured) == num_messages, 5, 0.5)
    finally:
        await producer.close()

    assert captured == [payload] * num_messages


async def test_send_batch_async_frames_never_exceed_frame_max(
    stream: str, consumer: Consumer, monkeypatch
) -> None:
    frame_max = 5000
    message_size = 1000
    num_messages = 40
    payload = b"x" * message_size

    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    sent_frame_sizes: list[int] = []
    original_send_publish_frame = Client.send_publish_frame

    async def spy_send_publish_frame(self, frame, version=1):
        # Measure the exact bytes that will be written on the wire for this
        # frame, the same way Connection.write_frame_publish() does.
        sent_frame_sizes.append(len(encode_publish(frame, version)))
        await original_send_publish_frame(self, frame, version)

    monkeypatch.setattr(Client, "send_publish_frame", spy_send_publish_frame)

    producer = Producer("localhost", username="guest", password="guest", frame_max=frame_max)
    await producer.start()
    try:
        for _ in range(num_messages):
            await producer.send(stream, payload)

        await wait_for(lambda: len(captured) == num_messages, 5, 0.5)
    finally:
        await producer.close()

    assert len(sent_frame_sizes) > 1, "expected the batch to be split across multiple frames"
    assert all(size <= frame_max for size in sent_frame_sizes)


async def test_publish_deduplication(stream: str, producer: Producer, consumer: Consumer) -> None:
    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    async def publish_with_ids(*ids):
        for publishing_id in ids:
            await producer.send_wait(
                stream=stream,
                message=RawMessage(f"test_{publishing_id}".encode(), publishing_id=publishing_id),
                publisher_name="MyProducerName",
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
                stream=stream,
                message=RawMessage(f"test_{publishing_id}".encode(), publishing_id=publishing_id),
                publisher_name="MyProducerName",
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
    conn_name = "test_producer_connection_broke_{}".format(time.time())
    streams_disconnected: list[str] = []

    async def on_close_connection(error_info: OnClosedErrorInfo):
        streams_disconnected.extend(error_info.streams)

    producer_broke = Producer(
        "localhost",
        username="guest",
        password="guest",
        connection_name=conn_name,
        on_close_handler=on_close_connection,
    )

    captured: list[bytes] = []
    await consumer.subscribe(
        stream, callback=lambda message, message_context: captured.append(bytes(message))
    )

    await producer_broke.start()
    # wait a bit to be sure that the connection is shown in the connections list HTTP API
    await asyncio.sleep(1)
    count = 0
    while True:
        await producer_broke.send(stream, b"one")
        count = count + 1
        if count % 100 == 0:
            await asyncio.sleep(0.2)
        if count == 200:
            await http_api_delete_connection_and_check(conn_name)
        if count >= 500:
            break

    await wait_for(lambda: len(streams_disconnected) == 1, 5, 1)
    await wait_for(lambda: len(captured) == 500, 5, 1)
    await producer_broke.close()


async def test_producer_reconnects_concurrently_for_streams_sharing_a_connection(monkeypatch) -> None:
    # Streams that share a broken connection must all start recovering at once.
    # If recovery were awaited sequentially, stream N+1 wouldn't even start
    # recovering until stream N's backoff/reconnect attempt (which can take
    # several seconds) finishes, so streams sharing a connection could be
    # starved of reconnection if the connection keeps getting killed faster
    # than the serialized chain of recoveries can complete.
    producer = Producer("localhost", username="guest", password="guest")

    shared_streams = ["shared-stream-0", "shared-stream-1", "shared-stream-2"]
    recovery_start_times: dict[str, float] = {}
    recovery_delay = 0.3

    async def fake_maybe_restart_producer(reason: str, stream: str) -> None:
        recovery_start_times[stream] = time.monotonic()
        await asyncio.sleep(recovery_delay)

    monkeypatch.setattr(producer, "maybe_restart_producer", fake_maybe_restart_producer)

    handler_start = time.monotonic()
    await producer._on_connection_closed(
        OnClosedErrorInfo(reason="Connection Closed", streams=shared_streams)
    )
    handler_duration = time.monotonic() - handler_start

    # the handler must return almost immediately: recovery is scheduled in the
    # background instead of being awaited one stream at a time
    assert handler_duration < recovery_delay

    await wait_for(lambda: len(recovery_start_times) == len(shared_streams), 2, 0.05)

    # all streams must start recovering together, not one after another
    spread = max(recovery_start_times.values()) - min(recovery_start_times.values())
    assert spread < recovery_delay


async def test_fail_pending_confirmations_reports_lost_messages_on_disconnect() -> None:
    # Messages that were sent (registered in _waiting_for_confirm) but whose
    # PublishConfirm/PublishError frame never arrives because the connection
    # dropped must still be reported to the caller as failed. Otherwise
    # on_publish_confirm is never called for them: they are neither confirmed
    # nor counted as errors, they just vanish, so confirmed_count silently
    # falls behind the number of messages actually sent.
    producer = Producer("localhost", username="guest", password="guest")

    confirmed: list[ConfirmationStatus] = []

    async def on_publish_confirm(confirmation: ConfirmationStatus) -> None:
        confirmed.append(confirmation)

    publisher_id = 0
    future: asyncio.Future = asyncio.get_event_loop().create_future()
    producer._waiting_for_confirm[publisher_id][on_publish_confirm] = {1, 2, 3}
    producer._waiting_for_confirm[publisher_id][future] = {4, 5}

    await producer._fail_pending_confirmations(publisher_id, "Connection Closed")

    assert {c.message_id for c in confirmed} == {1, 2, 3}
    assert all(c.is_confirmed is False for c in confirmed)

    assert future.done()
    with pytest.raises(exceptions.ServerError):
        future.result()

    # the entry must be cleared so a stale publisher_id can't leak state
    assert publisher_id not in producer._waiting_for_confirm


def _fake_publisher(stream: str, send_publish_frame) -> _Publisher:
    return _Publisher(
        id=0,
        reference=None,
        stream=stream,
        sequence=utils.MonotonicSeq(),
        client=SimpleNamespace(frame_max=1024 * 1024, send_publish_frame=send_publish_frame),  # type:ignore
    )


async def test_send_batch_does_not_drop_confirmation_racing_with_send(monkeypatch) -> None:
    # Regression test: on a fast/stable connection the broker can send back
    # PublishConfirm as soon as send_publish_frame is awaited, before this
    # coroutine gets a chance to run again. Confirmation tracking must be
    # registered in _waiting_for_confirm *before* the frame is sent, otherwise
    # the incoming PublishConfirm finds nothing to match and the callback is
    # silently never invoked even though the broker confirmed the message.
    producer = Producer("localhost", username="guest", password="guest")
    stream = "race-stream"

    async def fake_send_publish_frame(frame, version=1):
        publishing_ids = [m.publishing_id for m in frame.messages]
        await producer._on_publish_confirm(
            schema.PublishConfirm(publisher_id=frame.publisher_id, publishing_ids=publishing_ids),
            publisher,
        )

    publisher = _fake_publisher(stream, fake_send_publish_frame)

    async def fake_get_or_create_publisher(stream_name, publisher_name=None):
        return publisher

    monkeypatch.setattr(producer, "_get_or_create_publisher", fake_get_or_create_publisher)

    confirmed: list[ConfirmationStatus] = []

    async def on_publish_confirm(confirmation: ConfirmationStatus) -> None:
        confirmed.append(confirmation)

    await producer._send_batch(stream, [b"hello"], callback=on_publish_confirm, sync=False)

    assert len(confirmed) == 1
    assert confirmed[0].is_confirmed is True


async def test_send_async_does_not_drop_confirmation_racing_with_send(monkeypatch) -> None:
    # Same race as above but for the buffered/async path used by
    # Producer.send(), which is what BestPracticesClient.py relies on.
    producer = Producer("localhost", username="guest", password="guest")
    stream = "race-stream-async"

    async def fake_send_publish_frame(frame, version=1):
        publishing_ids = [m.publishing_id for m in frame.messages]
        await producer._on_publish_confirm(
            schema.PublishConfirm(publisher_id=frame.publisher_id, publishing_ids=publishing_ids),
            publisher,
        )

    publisher = _fake_publisher(stream, fake_send_publish_frame)

    async def fake_get_or_create_publisher(stream_name, publisher_name=None):
        return publisher

    monkeypatch.setattr(producer, "_get_or_create_publisher", fake_get_or_create_publisher)

    confirmed: list[ConfirmationStatus] = []

    async def on_publish_confirm(confirmation: ConfirmationStatus) -> None:
        confirmed.append(confirmation)

    batch = [_MessageNotification(entry=b"hello", callback=on_publish_confirm) for _ in range(50)]
    await producer._send_batch_async(stream, batch)

    assert len(confirmed) == 50
    assert all(c.is_confirmed is True for c in confirmed)


# flaky test
@pytest.mark.flaky(reruns=2, reruns_delay=1)
async def test_super_stream_producer_connection_broke(super_stream: str, consumer: Consumer) -> None:
    conn_name = "test_super_stream_producer_connection_broke_{}".format(time.time())
    streams_disconnected: list[str] = []

    async def on_close_connection(error_info: OnClosedErrorInfo):
        streams_disconnected.extend(error_info.streams)

    super_stream_producer_broke = SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor_generic,
        routing=RouteType.Hash,
        connection_name=conn_name,
        super_stream=super_stream,
        on_close_handler=on_close_connection,
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
        if count == 500:
            await http_api_delete_connection_and_check(conn_name)
        if count >= 1000:
            break

    await super_stream_producer_broke.close()
    await wait_for(lambda: len(captured_stream1) + len(captured_stream2) + len(captured_stream3) > 500, 15, 1)
    assert len(streams_disconnected) == 3
    assert super_stream + "-0" in streams_disconnected
    assert super_stream + "-1" in streams_disconnected
    assert super_stream + "-2" in streams_disconnected
