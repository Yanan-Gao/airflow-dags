from abc import ABC
from typing import Optional

from ttd.ttdenv import TtdEnvFactory


class Worker(ABC):

    def __init__(self, queue: str, pool: str, dev_worker: Optional["Worker"] = None):
        self._dev_worker = dev_worker

        if dev_worker is not None and TtdEnvFactory.get_from_system() == TtdEnvFactory.dev:
            self.original_queue = queue
            self.original_pool = pool
            queue = dev_worker.queue
            pool = dev_worker.pool

        self._queue = queue
        self._pool = pool

    def __str__(self) -> str:
        return (
                f"queue={self.queue}, pool={self.pool}" + f"in dev env; originals: '{self.original_queue}/{self.original_pool}'"
        ) if self._dev_worker is not None and TtdEnvFactory.get_from_system() == TtdEnvFactory.dev \
            else ""

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def queue(self):
        return self._queue

    @property
    def pool(self):
        return self._pool


class Celery(Worker):

    def __init__(self):
        super().__init__("default", "default_pool")


class CeleryChina(Worker):

    def __init__(self):
        super().__init__("default_china", "default_china_pool", Celery())


class Kubernetes(Worker):

    def __init__(self):
        super().__init__("k8s_executor", "k8s_executor", Celery())


class Workers:
    celery = Celery()
    celery_china = CeleryChina()
    k8s = Kubernetes()
