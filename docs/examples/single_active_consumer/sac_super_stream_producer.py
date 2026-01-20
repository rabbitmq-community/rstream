# rstream: python stream client for rabbitmq stream protocol
# rstream super stream producer example
# super stream documentation: https://www.rabbitmq.com/docs/streams#super-streams
# example path: https://github.com/rabbitmq-community/rstream/blob/master/docs/examples/single_active_consumer/sac_super_stream_producer.py
# more info about rabbitmq stream protocol: https://www.rabbitmq.com/docs/stream


import asyncio
import logging
import sys

from rstream._pyamqp.message import Properties  # type: ignore

print(sys.path)

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    RouteType,
    SuperStreamCreationOption,
    SuperStreamProducer,
)

counter = 0


async def routing_extractor(message: AMQPMessage) -> str:
    return str(message.properties.message_id)


logging.getLogger().setLevel(logging.INFO)
logging.basicConfig()


async def _on_publish_confirm(confirmation: ConfirmationStatus) -> None:
    global counter
    counter += 1
    if confirmation.is_confirmed:
        if (counter % 5) == 0:
            logging.info("confirmed: {} messages".format(counter))
    else:
        logging.error(
            "message id: {} not confirmed. Response code {}".format(
                confirmation.message_id, confirmation.response_code
            )
        )


async def publish():
    async with SuperStreamProducer(
        "localhost",
        username="guest",
        password="guest",
        super_stream="invoices",
        routing=RouteType.Hash,
        routing_extractor=routing_extractor,
        super_stream_creation_option=SuperStreamCreationOption(n_partitions=3),
    ) as producer:
        # run slowly several messages in order to test with sac
        for i in range(10_000_000):
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
                properties=Properties(message_id=str(i)),
            )
            await producer.send(message=amqp_message, on_publish_confirm=_on_publish_confirm)
            await asyncio.sleep(0.5)
            logging.info("sent: {} messages ".format(i + 1))

        await asyncio.sleep(5)


asyncio.run(publish())

logging.info("done")
