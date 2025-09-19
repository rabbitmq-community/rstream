from __future__ import annotations

from typing import Any, Optional, Protocol

from ._pyamqp._decode import decode_payload
from ._pyamqp._encode import encode_payload
from ._pyamqp.message import Message

# import uamqp


class _MessageProtocol(Protocol):
    publishing_id: Optional[int] = None

    def __bytes__(self) -> bytes:
        ...


class AMQPMessage(Message, _MessageProtocol):
    def __init__(self, *args: Any, publishing_id: Optional[int] = None, **kwargs: Any):
        self.publishing_id = publishing_id
        super().__init__(*args, **kwargs)

    def __bytes__(self) -> bytes:
        returned_value = bytearray()
        ret = encode_payload(output=returned_value, payload=self)
        return bytes(ret)

    def __str__(self) -> str:
        return str(self.body)


def amqp_decoder(data: bytes) -> AMQPMessage:
    message = decode_payload(buffer=memoryview(data))
    returned_amqp_message = AMQPMessage(
        value=message.value,
        application_properties=message.application_properties,
        properties=message.properties,
        message_annotations=message.message_annotations,
        footer=message.footer,
        header=message.header,
        delivery_annotations=message.delivery_annotations,
        sequence=message.sequence,
        body=message.body,
    )

    return returned_amqp_message
