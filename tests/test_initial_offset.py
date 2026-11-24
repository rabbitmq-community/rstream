import re
import time
from unittest.mock import AsyncMock

import pytest

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    Producer,
    exceptions,
    schema,
)
from rstream.client import Client, stream_arguments_with_initial_offset
from rstream.constants import MAX_STREAM_INITIAL_OFFSET, STREAM_INITIAL_OFFSET_ARGUMENT

from .util import wait_for

MIN_BROKER_VERSION = (4, 4, 0)


def _stream_name(prefix: str) -> str:
    return "{}_{}".format(prefix, time.time())


def _parse_version(version: str) -> tuple[int, ...]:
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
    return tuple(int(g) for g in match.groups()) if match else (0, 0, 0)


@pytest.fixture()
def require_initial_offset_support(client: Client) -> None:
    assert client.server_properties is not None
    version = client.server_properties.get("version", "0.0.0")
    if _parse_version(version) < MIN_BROKER_VERSION:
        pytest.skip(f"stream-initial-offset requires RabbitMQ >= 4.4.0, broker is {version}")


async def _first_offsets(consumer: Consumer, stream: str, expected: int) -> list[int]:
    """Subscribe from the start of the stream and collect the offsets of `expected` messages."""
    offsets: list[int] = []

    async def on_message(msg: AMQPMessage, message_context: MessageContext) -> None:
        offsets.append(message_context.offset)

    await consumer.subscribe(stream, callback=on_message)
    return offsets


# ---------------------------------------------------------------------------
# Parameter validation (no broker needed)
# ---------------------------------------------------------------------------


def test_max_initial_offset_matches_broker() -> None:
    assert MAX_STREAM_INITIAL_OFFSET == (1 << 62) - 1


def test_arguments_without_initial_offset_are_unchanged() -> None:
    assert stream_arguments_with_initial_offset(None, None) == {}
    assert stream_arguments_with_initial_offset({"max-age": "1h"}, None) == {"max-age": "1h"}


def test_arguments_are_not_mutated() -> None:
    original = {"max-age": "1h"}
    result = stream_arguments_with_initial_offset(original, 10)
    assert original == {"max-age": "1h"}
    assert result == {"max-age": "1h", STREAM_INITIAL_OFFSET_ARGUMENT: 10}


@pytest.mark.parametrize("offset", [0, 1, 100, 1_000_000, MAX_STREAM_INITIAL_OFFSET])
def test_valid_initial_offsets(offset: int) -> None:
    result = stream_arguments_with_initial_offset(None, offset)
    assert result == {"stream-initial-offset": offset}


@pytest.mark.parametrize("offset", [-1, -100, MAX_STREAM_INITIAL_OFFSET + 1, 1 << 63, 1 << 64])
def test_out_of_range_initial_offsets_raise(offset: int) -> None:
    with pytest.raises(ValueError, match="initial_offset must be between 0 and"):
        stream_arguments_with_initial_offset(None, offset)


@pytest.mark.parametrize("offset", ["10", 1.5, True, False, [1], b"1"])
def test_non_integer_initial_offsets_raise(offset) -> None:
    with pytest.raises(ValueError, match="initial_offset must be an integer"):
        stream_arguments_with_initial_offset(None, offset)


@pytest.mark.asyncio
async def test_client_create_stream_sends_initial_offset_as_string_property() -> None:
    client = Client(host="localhost", port=5552, frame_max=1024 * 1024, heartbeat=60)
    client.sync_request = AsyncMock()  # type: ignore[method-assign]

    await client.create_stream("s", arguments={"max-age": "1h"}, initial_offset=42)

    sent = client.sync_request.call_args.args[0]
    assert isinstance(sent, schema.Create)
    assert sent.stream == "s"
    assert {p.key: p.value for p in sent.arguments} == {"max-age": "1h", "stream-initial-offset": "42"}


@pytest.mark.asyncio
async def test_client_create_stream_validates_before_sending() -> None:
    client = Client(host="localhost", port=5552, frame_max=1024 * 1024, heartbeat=60)
    client.sync_request = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await client.create_stream("s", initial_offset=-1)
    with pytest.raises(ValueError):
        await client.create_stream("s", initial_offset=MAX_STREAM_INITIAL_OFFSET + 1)

    client.sync_request.assert_not_called()


@pytest.mark.asyncio
async def test_producer_create_stream_rejects_invalid_initial_offset(producer: Producer) -> None:
    with pytest.raises(ValueError):
        await producer.create_stream(_stream_name("invalid-offset"), initial_offset=-1)


@pytest.mark.asyncio
async def test_consumer_create_stream_rejects_invalid_initial_offset(consumer: Consumer) -> None:
    with pytest.raises(ValueError):
        await consumer.create_stream(
            _stream_name("invalid-offset"), initial_offset=MAX_STREAM_INITIAL_OFFSET + 1
        )


# ---------------------------------------------------------------------------
# Integration tests (RabbitMQ >= 4.4.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_create_stream_with_initial_offset(
    client: Client, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-client")
    await client.create_stream(stream, initial_offset=1000)
    try:
        assert await client.stream_exists(stream) is True
    finally:
        await client.delete_stream(stream)


@pytest.mark.asyncio
async def test_first_message_gets_initial_offset(
    producer: Producer, consumer: Consumer, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-first")
    initial_offset = 1_000
    await producer.create_stream(stream, initial_offset=initial_offset)
    try:
        offsets = await _first_offsets(consumer, stream, 3)
        await producer.send_batch(stream, [str(i).encode() for i in range(3)])

        await wait_for(lambda: len(offsets) >= 3)
        assert offsets == [initial_offset, initial_offset + 1, initial_offset + 2]
    finally:
        await producer.delete_stream(stream)


@pytest.mark.asyncio
async def test_initial_offset_zero_is_the_default(
    producer: Producer, consumer: Consumer, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-zero")
    await producer.create_stream(stream, initial_offset=0)
    try:
        offsets = await _first_offsets(consumer, stream, 1)
        await producer.send_wait(stream, b"hello")

        await wait_for(lambda: len(offsets) >= 1)
        assert offsets == [0]
    finally:
        await producer.delete_stream(stream)


@pytest.mark.asyncio
async def test_initial_offset_max_value(
    producer: Producer, consumer: Consumer, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-max")
    await producer.create_stream(stream, initial_offset=MAX_STREAM_INITIAL_OFFSET)
    try:
        offsets = await _first_offsets(consumer, stream, 1)
        await producer.send_wait(stream, b"hello")

        await wait_for(lambda: len(offsets) >= 1)
        assert offsets == [MAX_STREAM_INITIAL_OFFSET]
    finally:
        await producer.delete_stream(stream)


@pytest.mark.asyncio
async def test_consumer_create_stream_with_initial_offset(
    producer: Producer, consumer: Consumer, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-consumer")
    await consumer.create_stream(stream, initial_offset=50)
    try:
        offsets = await _first_offsets(consumer, stream, 1)
        await producer.send_wait(stream, b"hello")

        await wait_for(lambda: len(offsets) >= 1)
        assert offsets == [50]
    finally:
        await consumer.delete_stream(stream)


@pytest.mark.asyncio
async def test_initial_offset_combined_with_other_arguments(
    producer: Producer, consumer: Consumer, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-args")
    await producer.create_stream(stream, arguments={"max-length-bytes": 1_000_000_000}, initial_offset=7)
    try:
        offsets = await _first_offsets(consumer, stream, 1)
        await producer.send_wait(stream, b"hello")

        await wait_for(lambda: len(offsets) >= 1)
        assert offsets == [7]
    finally:
        await producer.delete_stream(stream)


@pytest.mark.asyncio
async def test_create_stream_with_different_initial_offset_fails(
    producer: Producer, require_initial_offset_support: None
) -> None:
    stream = _stream_name("initial-offset-conflict")
    await producer.create_stream(stream, initial_offset=10)
    try:
        # the initial offset is part of the stream definition: redeclaring with another value is refused
        with pytest.raises(exceptions.ServerError):
            await producer.create_stream(stream, initial_offset=20)
        # redeclaring with the same value is a no-op when exists_ok is set
        await producer.create_stream(stream, exists_ok=True, initial_offset=10)
    finally:
        await producer.delete_stream(stream)
