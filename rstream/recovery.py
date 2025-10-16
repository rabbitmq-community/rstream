# define the recovery strategy in case of connection failure or metadata update
import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Annotated, Any, Awaitable, Callable, TypeVar, Union

MT = TypeVar("MT")
CB = Annotated[Callable[[MT, Any], Union[None, Awaitable[None]]], "Message callback type"]
CB_CONN = Annotated[Callable[[MT], Union[None, Awaitable[Any]]], "Message callback type"]

# define the interface for recovery strategy


@dataclass
class IReliableEntity(ABC):
    @abstractmethod
    async def stream_exists(self, stream: str, on_close_event: bool = False) -> bool:
        pass

    def __init__(self):
        self._recovery_strategy: RecoveryStrategy


class RecoveryStrategy(ABC):
    def __init__(self, enable: bool = True, max_retries: int = 10):
        self._enable = enable
        self._max_retries = max_retries

    @abstractmethod
    async def recover(
        self, entity: IReliableEntity, stream: str, error: Exception, attempt: int, recovery_fun
    ) -> None:
        pass

    def enable(self):
        self._enable = True

    def disable(self):
        self._enable = False


@dataclass
class BackOffRecoveryStrategy(RecoveryStrategy):
    backoff_seconds: float = 1.0

    def __init__(self, enable: bool = True, jitter: int = 8):
        super().__init__(enable)
        self.backoff_seconds = 1.0
        self.jitter = jitter

    async def recover(
        self, entity: IReliableEntity, stream: str, error: Exception, attempt: int, recovery_fun
    ) -> None:
        if not self._enable:
            logging.info("[backOff recovery strategy] disabled, not recovering")
            return
        if attempt > self._max_retries:
            logging.info(
                "[backOff recovery strategy] max retries reached ({}), not recovering".format(
                    self._max_retries
                )
            )
            return
        logging.debug("[backOff recovery strategy] init for stream: {}".format(stream))
        # calculate the backoff time
        backoff_time = self.backoff_seconds * (2 ** (attempt - 1))
        delay = random.uniform(1, self.jitter)
        backoff_time = backoff_time + delay
        logging.debug(
            "[backOff recovery strategy] waiting for {} seconds before retrying".format(backoff_time)
        )
        # add safe jitter with some randomization
        await asyncio.sleep(backoff_time)
        time.sleep(backoff_time)
        try:
            # check if the stream still exists
            stream_exist = await entity.stream_exists(stream)
            logging.debug("[backOff recovery strategy] stream: {} exist {}".format(stream, stream_exist))
            if stream_exist:
                logging.debug(
                    "[backOff recovery strategy] stream: {} exists, trying to recover".format(stream)
                )
                await recovery_fun()
                self.backoff_seconds = 1.0
            else:
                logging.debug(
                    "[backOff recovery strategy] stream: {} does not exist. Recovery stopped".format(stream)
                )
        except Exception as ex:
            logging.error(
                "[backOff recovery strategy] error checking stream existence or recovering: {}".format(ex)
            )
            self.backoff_seconds = self.backoff_seconds * 2
            # retry again
            await self.recover(entity, stream, ex, attempt + 1, recovery_fun)


#     TODO: check the error type to decide if we need to retry or not
#     if errors.Is(errS, stream.StreamNotAvailable) {
#     logs.LogInfo("[Reliable] - The stream %s is not available
#     for %s. Trying to reconnect", streamName, reliable.getInfo())
#     return retry(backoff + 1, reliable, streamName)
#     }
#     if errors.Is(errS, stream.LeaderNotReady) {
#     logs.LogInfo("[Reliable] - The leader for the stream %s is not ready
#     for %s. Trying to reconnect", streamName,
#                  reliable.getInfo())
#     return retry(backoff + 1, reliable, streamName)
#
# }
