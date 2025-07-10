from typing import Any, TYPE_CHECKING

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import (
    KubernetesExecutor,
)
from airflow.models.xcom import XCom

from ttd.kubernetes.pod_resources import PodResources

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType


class TtdKubernetesExecutor(KubernetesExecutor):

    def __init__(self):
        super().__init__()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: "CommandType",
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        pod_resources_dict = XCom.get_one(
            key="pod_resources",
            dag_id=key.dag_id,
            task_id=key.task_id,
            run_id=key.run_id,
        )

        if pod_resources_dict is not None:
            executor_config = PodResources.from_dict(**pod_resources_dict).as_executor_config()

        super().execute_async(
            key,
            command,
            queue,
            executor_config,
        )
