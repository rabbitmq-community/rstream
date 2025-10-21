from __future__ import annotations

from typing import Type


class ServerError(Exception):
    code: int = NotImplemented
    _registry: dict[int, Type[ServerError]] = {}

    def __init_subclass__(cls) -> None:
        cls._registry[cls.code] = cls

    @classmethod
    def from_code(cls, code: int) -> Type[ServerError]:
        return cls._registry.get(code, cls)


class ClientError(Exception):
    pass


class StreamDoesNotExist(ServerError):
    code = 2


class SubscriptionIDAlreadyExists(ServerError):
    code = 3


class SubscriptionIDDoesNotExist(ServerError):
    code = 4


class StreamAlreadyExists(ServerError):
    code = 5


class StreamNotAvailable(ServerError):
    code = 6


class SASLMechanismNotSupported(ServerError):
    code = 7


class AuthenticationFailure(ServerError):
    code = 8


class SASLError(ServerError):
    code = 9


class SASLChallenge(ServerError):
    code = 10


class SASLAuthenticationFailureLoopback(ServerError):
    code = 11


class VirtualHostAccessFailure(ServerError):
    code = 12


class UnknownFrame(ServerError):
    code = 13


class FrameTooLarge(ServerError):
    code = 14


class InternalError(ServerError):
    code = 15


class AccessRefused(ServerError):
    code = 16


class PreconditionFailed(ServerError):
    code = 17


class PublisherDoesNotExist(ServerError):
    code = 18


class OffsetNotFound(ServerError):
    code = 19


class MaxConsumersPerInstance(ClientError):
    pass


class MaxPublishersPerInstance(ClientError):
    pass


class StreamAlreadySubscribed(ClientError):
    pass


class LeaderNotAvailable(ClientError):
    pass
