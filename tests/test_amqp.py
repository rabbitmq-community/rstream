# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import pytest

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    Producer,
    Properties,
    amqp_decoder,
)

pytestmark = pytest.mark.asyncio


async def test_amqp_message(producer: Producer, consumer: Consumer, stream: str) -> None:
    amqp_message = AMQPMessage(
        properties=Properties(subject=b"test-subject"),
        message_annotations={b"test": 42},
        body=b"test-body",
    )
    await producer.send_wait(stream, amqp_message)

    incoming_amqp_message = None

    def callback(msg: AMQPMessage, message_context: MessageContext):
        nonlocal incoming_amqp_message
        incoming_amqp_message = msg
        consumer.stop()

    await consumer.subscribe(stream, callback=callback, decoder=amqp_decoder)
    await consumer.run()

    assert isinstance(incoming_amqp_message, AMQPMessage)
    assert list(incoming_amqp_message.body) == list(amqp_message.body)
    assert incoming_amqp_message.properties.subject == amqp_message.properties.subject
    assert incoming_amqp_message.message_annotations == amqp_message.message_annotations
