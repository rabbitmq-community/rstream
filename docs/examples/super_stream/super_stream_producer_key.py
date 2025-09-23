import asyncio
import time

from rstream import (
    AMQPMessage,
    RouteType,
    SuperStreamCreationOption,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 1000000


# routing key and the bindings between the super stream exchange and the streams
async def routing_extractor(message: AMQPMessage) -> str:
    return "key1"


async def publish():
    # SuperStreamProducer wraps a Producer
    # Struct used to pass information to the SuperStreamProducer
    # in order to create a superstream
    super_stream_creation_opt = SuperStreamCreationOption(
        n_partitions=0, binding_keys=["key1", "key2", "key3"]
    )

    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        routing=RouteType.Key,
        super_stream=SUPER_STREAM,
    ) as super_stream_producer:
        start_time = time.perf_counter()
        # Sending a million messages
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                application_properties={"id": "{}".format(i)},
            )
            await super_stream_producer.send(amqp_message)
            if i % 100000 == 0:
                print(f"Published {i} messages to super stream: {SUPER_STREAM}")

        end_time = time.perf_counter()
        print(
            f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds to super stream: {SUPER_STREAM}"
        )


asyncio.run(publish())
