from abc import ABC, abstractmethod
from functools import cache
from typing import List, Dict, Union, Optional, Tuple

from ttd.eldorado.databricks.task_config import DatabricksTaskConfig
from ttd.eldorado.xcom.helpers import get_push_xcom_task_id, get_xcom_pull_jinja_string


class DatabricksJobTask(ABC):
    """
    @param depends_on: A list of databricks tasks that this one depends on
    @param name: The original name given by the user
    @param task_name: The task name in databricks, which typically the same as name with some suffix
    @param do_xcom_push: When do_xcom_push set to True DatabricksWorkflow will create tasks to
                         capture output of task and push to Xcom, see DatabricksPushXcomOperator
    """
    depends_on: List['DatabricksJobTask']
    name: str
    task_name: str
    do_xcom_push: bool = False

    @abstractmethod
    def task_parameters(self) -> List[str]:
        pass

    @abstractmethod
    @cache
    def get_main_task(self, cluster_key: str) -> DatabricksTaskConfig:
        pass

    def _expand_parameters(self) -> List[str]:
        return ["{{ '{{ job.parameters." + f"`{self.task_name}-{i}`" + "}}' }}" for i in range(len(self.task_parameters()))]

    @abstractmethod
    def configure_openlineage(
        self, cluster_key: str, cluster_name: str, tasks: List[DatabricksTaskConfig], init_scripts: List[Dict[str, Dict[str, str]]],
        spark_configs: Dict[str, str]
    ):
        pass

    def __rshift__(self, other: Union['DatabricksJobTask',
                                      List['DatabricksJobTask']]) -> Union['DatabricksJobTask', List['DatabricksJobTask']]:
        if isinstance(other, DatabricksJobTask):
            other.depends_on.append(self)

        if isinstance(other, list):
            for item in other:
                self >> item

        return other

    def get_push_xcom_task_id(self) -> str:
        """
        Get the task id for the push xcom task
        """
        return get_push_xcom_task_id(self.name)

    def get_xcom_pull_jinja_string(self, key: str) -> str:
        """
        Gets a jinja string for a specific key from task output
        """
        if not self.do_xcom_push:
            raise AssertionError("do_xcom_push must be set to True to be able to pull")
        return get_xcom_pull_jinja_string(self.get_push_xcom_task_id(), key)


class RunLevelJvmParameterParser:

    @staticmethod
    def parse(args: Optional[List[Tuple[str, str]]]) -> List[str]:
        return [f"{key}={value}" for key, value in args] if args else []
