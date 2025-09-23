import asyncio
import sys

import uamqp

print(sys.path)


from rstream import (
    AMQPMessage,
    CompressionType,
    ConfirmationStatus,
    Producer,
    RouteType,
    SuperStreamProducer,
)

global counter


async def routing_extractor(message: AMQPMessage) -> str:
    return str(message.properties.message_id)


async def publish():
    global counter
    # counter = 0
    # sent = 0
    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        super_stream="invoices",
        routing=RouteType.Hash,
        routing_extractor=routing_extractor,
    ) as producer:
        messages = []
        # run slowly several messages in order to test with sac
        for i in range(1000000):
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                properties=uamqp.message.MessageProperties(message_id=i),
            )
            await producer.send(message=amqp_message)
            await asyncio.sleep(1)
            print("sent: {} messages ".format(i + 1))

        await asyncio.sleep(5)


asyncio.run(publish())

print("done")
