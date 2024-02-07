import asyncio
import time

from rstream import (
    AMQPMessage,
    OnClosedErrorInfo,
    RouteType,
    StreamDoesNotExist,
    SuperStreamProducer,
)

SUPER_STREAM = "invoices"
MESSAGES = 100000000
producer_closed = False


# this value will be hashed using mumh3 hashing algorithm to decide the partition resolution for the message
async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["id"]


async def publish():
    async def on_metadata_update(on_closed_info: OnClosedErrorInfo) -> None:

        print(
            "connection has been closed from stream: "
            + str(on_closed_info.streams)
            + " for reason: "
            + str(on_closed_info.reason)
        )

        await asyncio.sleep(2)
        # reconnect just if the partition exists
        for stream in on_closed_info.streams:
            # backoff = 1
            while True:
                try:
                    print("reconnecting stream: {}".format(stream))
                    global _lock
                    async with _lock:
                        await super_stream_producer.reconnect_stream(stream)
                    break
                except StreamDoesNotExist:
                    print("stream does not exist anymore")
                    continue
                except Exception as ex:
                    # if backoff > 32:
                    # failed to found the leader
                    # print("reconnection failed")
                    # break
                    # backoff = backoff * 2
                    print("exception reconnecting waiting 120s: " + str(ex))
                    await asyncio.sleep(30)
                    continue

        global producer_closed
        producer_closed = True

    # SuperStreamProducer wraps a Producer
    async with SuperStreamProducer(
            "34.105.232.133",
            username="test",
            password="test",
            routing_extractor=routing_extractor,
            routing=RouteType.Hash,
            super_stream=SUPER_STREAM,
            # on_close_handler=on_metadata_update,
            load_balancer_mode=True,
    ) as super_stream_producer:
        # Sending a million messages

        start_time = time.perf_counter()
        for i in range(MESSAGES):
            amqp_message = AMQPMessage(
                body="hello: {}".format(i),
                application_properties={"id": "{}".format(i)},
            )
            global producer_closed

            if producer_closed is False:
                try:

                    await super_stream_producer.send(amqp_message)
                    await asyncio.sleep(0.5)
                    print("sent message: {}".format(i))
                    # time.sleep(5)
                except Exception as e:
                    # give some time to the reconnect_stream to reconnect
                    print("error sending message: {}".format(e))
                    await asyncio.sleep(5)
                    # producer_closed = False
                    # continue
            else:
                # await asyncio.sleep()
                producer_closed = False
                continue
            # await asyncio.sleep(1)
            if i % 100000 == 0:
                print(f"Published {i} messages to super stream: {SUPER_STREAM}")

        print("FineFineFineFineFineFineFineFineFineFineFineFineFineFineFineFineFine")
        end_time = time.perf_counter()
        print(
            f"Sent {MESSAGES} messages in {end_time - start_time:0.4f} seconds to super stream: {SUPER_STREAM}"
        )


asyncio.run(publish())