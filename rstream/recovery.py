# define the recovery strategy in case of connection failure or metadata update
import asyncio
import logging
import random
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

    async def clean_list(self, stream: str) -> None:
        pass

    def __init__(self):
        self._recovery_strategy: RecoveryStrategy


class RecoveryStrategy(ABC):
    def __init__(self, enable: bool = False, max_retries: int = 10):
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

    def __init__(self, enable: bool = False, jitter: int = 8):
        super().__init__(enable)
        self.backoff_seconds = 1.0
        self.jitter = jitter

    async def recover(
        self, entity: IReliableEntity, stream: str, error: Exception, attempt: int, recovery_fun
    ) -> None:
        if not self._enable:
            logging.info(
                "[{}, backOff recovery strategy] disabled, not recovering".format(entity.__class__.__name__)
            )
            return
        if attempt > self._max_retries:
            logging.info(
                "[{},backOff recovery strategy] max retries reached ({}), not recovering".format(
                    entity.__class__.__name__, self._max_retries
                )
            )
            return
        logging.debug(
            "[{}, backOff recovery strategy] init for stream: {}. Attempt:{}, reason:{}".format(
                entity.__class__.__name__, stream, attempt, error.__class__.__name__
            )
        )
        # calculate the backoff time

        backoff_time = self.backoff_seconds * (2 ** (attempt - 1))
        if backoff_time > 50:
            backoff_time = 50

        delay = random.uniform(1, self.jitter)
        backoff_time = backoff_time + delay

        logging.debug(
            "[{}, backOff recovery strategy] waiting for {} seconds before retrying. Attempt:{}, reason:{}".format(
                entity.__class__.__name__, backoff_time, attempt, error.__class__.__name__
            )
        )

        # add safe jitter with some randomization
        await asyncio.sleep(backoff_time)
        try:
            # check if the stream still exists
            stream_exist = await entity.stream_exists(stream)
            logging.debug(
                "[{}, backOff recovery strategy] stream: {} exist {}. Attempt:{}, reason:{}".format(
                    entity.__class__.__name__, stream, stream_exist, attempt, error.__class__.__name__
                )
            )
            if stream_exist:
                logging.debug(
                    "[{}, backOff recovery strategy] stream: {} exists, trying to recover. "
                    "Attempt:{}, reason:{}".format(
                        entity.__class__.__name__, stream, attempt, error.__class__.__name__
                    )
                )
                await entity.clean_list(stream)
                await recovery_fun()
                logging.debug(
                    "[{}, backOff recovery strategy] stream: {} recovered successfully. Attempt:{}, reason:{}".format(
                        entity.__class__.__name__, stream, attempt, error.__class__.__name__
                    )
                )
                self.backoff_seconds = 1.0
                return
            else:
                logging.debug(
                    "[{},backOff recovery strategy] stream: {} does not exist. "
                    "Recovery stopped. Attempt:{}, reason:{}".format(
                        entity.__class__.__name__, stream, attempt, error.__class__.__name__
                    )
                )
        except Exception as ex:
            logging.error(
                "[{}, backOff recovery strategy] error checking stream"
                " existence or recovering: {}. "
                "Attempt:{}, reason:{}".format(
                    entity.__class__.__name__, ex, attempt, error.__class__.__name__
                )
            )
            self.backoff_seconds = self.backoff_seconds * 2
            # retry again
            await self.recover(entity, stream, ex, attempt + 1, recovery_fun)
