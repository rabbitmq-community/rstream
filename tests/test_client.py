from unittest.mock import ANY

import pytest

from rstream import schema
from rstream.client import Client
from rstream.constants import Key

pytestmark = pytest.mark.asyncio


async def test_peer_properties(no_auth_client: Client) -> None:
    result = await no_auth_client.peer_properties()
    assert result["product"] == "RabbitMQ"


async def test_create_stream(client: Client) -> None:
    assert await client.stream_exists("test-stream") is False
    await client.create_stream("test-stream")
    assert await client.stream_exists("test-stream") is True
    await client.delete_stream("test-stream")
    assert await client.stream_exists("test-stream") is False


async def test_deliver(client: Client, stream: str) -> None:
    subscription_id = 1
    publisher_id = 1
    await client.declare_publisher(stream, "test-reference", publisher_id)
    await client.subscribe(stream, subscription_id)

    waiter = client.wait_frame(schema.Deliver)
    msg = schema.Message(publishing_id=1, filter_value=None, data=b"test message")
    await client.publish([msg], publisher_id)

    assert await waiter == schema.Deliver(
        subscription_id=subscription_id,
        magic_version=80,
        chunk_type=0,
        num_entries=1,
        num_records=1,
        timestamp=ANY,
        epoch=1,
        chunk_first_offset=0,
        chunk_crc=307778378,
        data_length=16,
        trailer_length=24,
        _reserved=0,
        data=b"\x00\x00\x00\x0ctest message",
    )
    await client.credit(subscription_id, 1)
    await client.unsubscribe(subscription_id)
    await client.delete_publisher(publisher_id)


async def test_query_leader(client: Client, stream: str) -> None:
    leader, _ = await client.query_leader_and_replicas(stream)
    assert (leader.host, int(leader.port)) == (client.host, int(client.port))


async def test_partitions(client: Client) -> None:
    # create an exchange to connect the 3 supersteams
    stream = "test-stream"
    await client.create_super_stream(
        stream,
        [stream + "-0", stream + "-1", stream + "-2"],
        ["0", "1", "2"],
    )

    partitions = await client.partitions(super_stream=stream)

    await client.delete_super_stream(stream)

    assert len(partitions) == 3
    assert partitions[0] == "test-stream-0"
    assert partitions[1] == "test-stream-1"
    assert partitions[2] == "test-stream-2"


async def test_routes(client: Client) -> None:
    stream = "test-stream"
    await client.create_super_stream(
        stream,
        [stream + "-0", stream + "-1", stream + "-2"],
        ["test1", "test2", "test3"],
    )

    partitions = await client.route(super_stream=stream, routing_key="test1")

    await client.delete_super_stream(stream)

    assert len(partitions) == 1
    assert partitions[0] == "test-stream-0"


async def exchange_command_versions(client: Client) -> None:
    expected_min_version = 1
    expected_max_version = 1
    command_version_input = schema.FrameHandlerInfo(
        Key.Publish.value, min_version=expected_min_version, max_version=expected_min_version
    )
    command_version_server = await client.exchange_command_version(command_version_input)

    assert command_version_server.key_command == Key.Publish.value
    assert command_version_server.min_version == expected_min_version
    assert command_version_server.max_version == expected_max_version
