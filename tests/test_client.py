from typing import cast
from unittest.mock import ANY, AsyncMock

import pytest

from rstream import schema
from rstream.client import Client, ClientPool
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


class _FakeClient:
    """Stand-in for a real Client that ClientPool.get() can manage without a broker."""

    def __init__(self, alive: bool = True) -> None:
        self._alive = alive
        self.is_started = True

    def is_connection_alive(self) -> bool:
        return self._alive

    async def get_count_available_ids(self) -> int:
        return 1

    @property
    def is_locator(self) -> bool:
        return False

    def add_stream(self, stream: str) -> None:
        pass


def _make_client_pool() -> ClientPool:
    return ClientPool(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
        frame_max=1024 * 1024,
        heartbeat=60,
        load_balancer_mode=False,
        max_retries=3,
    )


def _as_client(client: _FakeClient) -> Client:
    """Type helper: _FakeClient only implements the subset of Client that
    ClientPool.get() touches, so seed it into the pool as a Client."""
    return cast(Client, client)


async def test_client_pool_evicts_closed_clients_on_get() -> None:
    """A closed client left in the pool (e.g. a locator connection closed after a
    stream_exists() query) is evicted before a new client is created."""
    pool = _make_client_pool()
    addr = pool.addr
    pool._clients[addr].append(_as_client(_FakeClient(alive=False)))

    async def fake_new(**kwargs: object) -> _FakeClient:
        return _FakeClient(alive=True)

    pool.new = AsyncMock(side_effect=fake_new)  # type: ignore[method-assign]

    for _ in range(5):
        await pool.get(connection_name=None)

    clients = pool._clients[addr]
    assert len(clients) == 1
    assert clients[0].is_connection_alive() is True
    # only the first get() needed to create a client; the rest reused it
    assert pool.new.await_count == 1


async def test_client_pool_reuses_alive_client() -> None:
    """An alive client is reused instead of appending a new one."""
    pool = _make_client_pool()
    addr = pool.addr
    live = _as_client(_FakeClient(alive=True))
    pool._clients[addr].append(live)

    pool.new = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("new() must not be called when a client can be reused")
    )

    for _ in range(3):
        got = await pool.get(connection_name=None)
        assert got is live

    assert len(pool._clients[addr]) == 1
    pool.new.assert_not_awaited()


async def test_client_pool_evicts_only_closed_clients() -> None:
    """Dead clients are dropped but live ones are still reused."""
    pool = _make_client_pool()
    addr = pool.addr
    live = _as_client(_FakeClient(alive=True))
    pool._clients[addr] = [_as_client(_FakeClient(alive=False)), live]

    pool.new = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("new() must not be called when a client can be reused")
    )

    got = await pool.get(connection_name=None)

    assert got is live
    assert pool._clients[addr] == [live]


async def test_client_pool_does_not_grow_across_open_close_cycles() -> None:
    """Reproduces the Producer.stream_exists() pattern: every call opens a short-lived
    locator client and closes it right after. The pool must not accumulate one dead
    Client per call."""
    pool = _make_client_pool()
    addr = pool.addr
    created: list[_FakeClient] = []

    async def fake_new(**kwargs: object) -> _FakeClient:
        client = _FakeClient(alive=True)
        created.append(client)
        return client

    pool.new = AsyncMock(side_effect=fake_new)  # type: ignore[method-assign]

    for _ in range(100):
        await pool.get(connection_name=None)
        # emulate _close_locator_connection(): a locator holds no streams and is
        # closed right after the metadata query. Each get() creates a fresh client
        # because the previous one was marked dead here and evicted on the next call.
        created[-1]._alive = False

    # a fresh connection is opened on every cycle...
    assert len(created) == 100
    # ...but the closed clients are evicted instead of being retained forever
    assert len(pool._clients[addr]) == 1
    assert pool._clients[addr][0] is created[-1]
