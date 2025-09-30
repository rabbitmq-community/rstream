import logging
import ssl

import pytest_asyncio

import rstream.client
from rstream import (
    Consumer,
    Producer,
    RouteType,
    SuperStreamConsumer,
    SuperStreamProducer,
)
from rstream.client import Client

from .util import (
    filter_value_extractor,
    routing_extractor,
    routing_extractor_for_sac,
    routing_extractor_key,
)

logging.basicConfig(
    level=logging.WARNING,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


@pytest_asyncio.fixture(scope="session", autouse=True)
def configure():
    rstream.client.DEFAULT_REQUEST_TIMEOUT = 1


def pytest_addoption(parser):
    parser.addoption("--rmq-host", action="store", default="localhost")
    parser.addoption("--rmq-port", action="store", default=5552)
    parser.addoption("--rmq-ssl", action="store", type=bool, default=False)
    parser.addoption("--rmq-vhost", action="store", default="/")
    parser.addoption("--rmq-username", action="store", default="guest")
    parser.addoption("--rmq-password", action="store", default="guest")
    # options for cluster testing. we keep it separate to avoid confusion
    parser.addoption("--rmq-cluster-host", action="store", default="localhost")
    parser.addoption("--rmq-cluster-port", action="store", default=5553)
    parser.addoption("--rmq-cluster-ssl", action="store", type=bool, default=False)
    parser.addoption("--rmq-cluster-vhost", action="store", default="/")
    parser.addoption("--rmq-cluster-username", action="store", default="guest")
    parser.addoption("--rmq-cluster-password", action="store", default="guest")
    parser.addoption("--rmq-cluster-load-balancer", action="store", type=bool, default=True)


@pytest_asyncio.fixture()
def ssl_context(pytestconfig):
    if pytestconfig.getoption("rmq_ssl"):
        return ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    else:
        return None


@pytest_asyncio.fixture()
async def no_auth_client(pytestconfig, ssl_context):
    rstream.client.DEFAULT_REQUEST_TIMEOUT = 1
    client = Client(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        frame_max=1024 * 1024,
        heartbeat=60,
        ssl_context=ssl_context,
    )
    await client.start()
    try:
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture()
async def client(no_auth_client: Client, pytestconfig):
    await no_auth_client.authenticate(
        vhost=pytestconfig.getoption("rmq_vhost"),
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
    )
    return no_auth_client


@pytest_asyncio.fixture()
async def stream(client: Client):
    try:
        await client.delete_stream("test-stream")
    except Exception:
        # it doesn't matter if it fails
        pass

    await client.create_stream("test-stream")
    try:
        yield "test-stream"
    finally:
        await client.delete_stream("test-stream")


@pytest_asyncio.fixture()
async def stream2(client: Client):
    await client.create_stream("test-stream-2")
    try:
        yield "test-stream-2"
    finally:
        await client.delete_stream("test-stream-2")


@pytest_asyncio.fixture()
async def consumer(pytestconfig, ssl_context):
    consumer = Consumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest_asyncio.fixture()
async def producer(pytestconfig, ssl_context):
    producer = Producer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest_asyncio.fixture()
async def producer_with_filtering(pytestconfig, ssl_context):
    producer = Producer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        filter_value_extractor=filter_value_extractor,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest_asyncio.fixture()
async def super_stream(client: Client):
    try:
        await client.delete_super_stream("test-super-stream")
    except Exception:
        # it doesn't matter if it fails
        pass

    await client.create_super_stream(
        "test-super-stream",
        ["test-super-stream-0", "test-super-stream-1", "test-super-stream-2"],
        ["key1", "key2", "key3"],
    )
    try:
        yield "test-super-stream"
    finally:
        await client.delete_super_stream("test-super-stream")


@pytest_asyncio.fixture()
async def super_stream_producer(pytestconfig, ssl_context):
    producer = SuperStreamProducer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        routing=RouteType.Hash,
        routing_extractor=routing_extractor,
        super_stream="test-super-stream",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest_asyncio.fixture()
async def super_stream_key_routing_producer(pytestconfig, ssl_context):
    producer = SuperStreamProducer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        routing=RouteType.Key,
        routing_extractor=routing_extractor_key,
        super_stream="test-super-stream",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest_asyncio.fixture()
async def super_stream_producer_for_sac(pytestconfig, ssl_context):
    producer = SuperStreamProducer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        routing=RouteType.Hash,
        routing_extractor=routing_extractor_for_sac,
        super_stream="test-super-stream",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest_asyncio.fixture()
async def super_stream_consumer(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest_asyncio.fixture()
async def super_stream_consumer_for_sac1(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest_asyncio.fixture()
async def super_stream_consumer_for_sac2(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest_asyncio.fixture()
async def super_stream_consumer_for_sac3(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


@pytest_asyncio.fixture()
async def super_stream_consumer_for_sac4(pytestconfig, ssl_context):
    consumer = SuperStreamConsumer(
        host=pytestconfig.getoption("rmq_host"),
        port=pytestconfig.getoption("rmq_port"),
        ssl_context=ssl_context,
        username=pytestconfig.getoption("rmq_username"),
        password=pytestconfig.getoption("rmq_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        super_stream="test-super-stream",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()


# cluster fixtures


@pytest_asyncio.fixture()
async def cluster_producer(pytestconfig):
    producer = Producer(
        host=pytestconfig.getoption("rmq_cluster_host"),
        port=pytestconfig.getoption("rmq_cluster_port"),
        ssl_context=None,
        username=pytestconfig.getoption("rmq_cluster_username"),
        password=pytestconfig.getoption("rmq_cluster_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        load_balancer_mode=pytestconfig.getoption("rmq_cluster_load_balancer"),
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.close()


@pytest_asyncio.fixture()
async def cluster_consumer(pytestconfig):
    consumer = Consumer(
        host=pytestconfig.getoption("rmq_cluster_host"),
        port=pytestconfig.getoption("rmq_cluster_port"),
        ssl_context=None,
        username=pytestconfig.getoption("rmq_cluster_username"),
        password=pytestconfig.getoption("rmq_cluster_password"),
        frame_max=1024 * 1024,
        heartbeat=60,
        load_balancer_mode=pytestconfig.getoption("rmq_cluster_load_balancer"),
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.close()
