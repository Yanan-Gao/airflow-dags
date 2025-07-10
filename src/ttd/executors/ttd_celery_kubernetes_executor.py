from airflow.providers.celery.executors.celery_kubernetes_executor import (
    CeleryKubernetesExecutor,
)
from airflow.providers.celery.executors.celery_executor import CeleryExecutor

from ttd.executors.ttd_kubernetes_executor import TtdKubernetesExecutor


class TtdCeleryKubernetesExecutor(CeleryKubernetesExecutor):

    def __init__(self):
        super().__init__(
            celery_executor=CeleryExecutor(),
            kubernetes_executor=TtdKubernetesExecutor(),
        )
