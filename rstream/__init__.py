# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

"""
isort:skip_file
"""

from importlib import metadata  # noqa: E402

from .utils import FilterConfiguration, OnClosedErrorInfo  # noqa: E402

from .amqp import AMQPMessage, amqp_decoder  # noqa: E402
from ._pyamqp.message import Properties  # type: ignore
from ._pyamqp.message import Header  # type: ignore
from .compression import CompressionType  # type: ignore
from .constants import (  # noqa: E402
    ConsumerOffsetSpecification,
    SlasMechanism,
)

try:
    __version__ = metadata.version(__package__)
    __license__ = metadata.metadata(__package__)["license"]
except metadata.PackageNotFoundError:
    __version__ = "dev"
    __license__ = "MIT"

del metadata


from .consumer import (  # noqa: E402
    Consumer,
    EventContext,
    MessageContext,
)
from .exceptions import OffsetNotFound  # noqa: E402
from .exceptions import ServerError  # noqa: E402
from .exceptions import StreamDoesNotExist  # noqa: E402
from .producer import (  # noqa: E402
    ConfirmationStatus,
    Producer,
    RawMessage,
)
from .schema import OffsetSpecification  # noqa: E402
from .superstream_consumer import (  # noqa: E402
    SuperStreamConsumer,
)
from .superstream_producer import (  # noqa: E402
    RouteType,
    SuperStreamProducer,
)

from .superstream import SuperStreamCreationOption  # noqa: E402; noqa: E402

from .constants import OffsetType  # noqa: E402; noqa: E402

__all__ = [
    "AMQPMessage",
    "amqp_decoder",
    "Consumer",
    "RawMessage",
    "Producer",
    "Properties",
    "Header",
    "OffsetType",
    "ConsumerOffsetSpecification",
    "ConfirmationStatus",
    "CompressionType",
    "SuperStreamProducer",
    "RouteType",
    "SuperStreamConsumer",
    "MessageContext",
    "ServerError",
    "OffsetNotFound",
    "StreamDoesNotExist",
    "OffsetSpecification",
    "EventContext",
    "OnClosedErrorInfo",
    "SlasMechanism",
    "FilterConfiguration",
    "SuperStreamCreationOption",
]
