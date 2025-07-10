import math
import time
from abc import ABC
from random import random
from typing import Callable, TypeVar

from airflow.utils.log.logging_mixin import LoggingMixin

from ttd.monads.trye import Try, Failure

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


class RetryMixin(LoggingMixin, ABC):
    serialized_fields = frozenset({"retry_interval", "max_retries", "exponential_retry"})

    def __init__(
        self,
        max_retries: int = 5,
        retry_interval: int = 2,
        exponential_retry: bool = True,
        *args,
        **kwargs,
    ):
        """

        :param max_retries: Number of times RetryMixin will retry the target method once it fails.
        :param retry_interval: The base number of seconds that is going to be used for exponential function to calculate delay for each
         interval
        :param exponential_retry:
        """
        super().__init__(*args, **kwargs)
        self.max_tries = max_retries + 1
        self.retry_interval = retry_interval
        self.exponential_retry = exponential_retry

    def with_retry(self, f: Callable[[], T], condition: Callable[[E], bool]) -> Try[T]:
        attempt_number = 0
        while True:
            attempt_number += 1

            attempt = Try.apply(f)
            if attempt.is_success:
                return attempt

            ex = attempt.failed().get()
            self.log.debug(f"Error: {str(ex)} == {str(ex.__dict__)}")
            if condition(ex):
                sleep_time = self.calc_sleep(attempt_number, self.retry_interval, self.exponential_retry)
                if attempt_number < self.max_tries:
                    self.log.info(
                        f"Retrying on {self.__class__.__name__} call attempt "
                        f"{attempt_number} out of {self.max_tries}, retrying"
                    )
                    time.sleep(sleep_time)
                else:
                    self.log.error(f"{self.__class__.__name__} call attempt beyond limits, returning failure.")
                    return Failure(RetryLimitException("Retry attempt limit reached", ex))
            else:
                return attempt

    @staticmethod
    def calc_sleep(attempt_number: int, retry_interval: int, exponential_retry: bool) -> int:
        if exponential_retry:
            next_interval = retry_interval**attempt_number - retry_interval**(attempt_number - 1)
            random_interval = math.ceil(next_interval * random())
            return retry_interval**(attempt_number - 1) + random_interval
        else:
            return retry_interval * attempt_number


class RetryLimitException(Exception):

    def __init__(self, msg: str, ex: Exception):
        super().__init__(msg)
        self.nested_exception = ex
