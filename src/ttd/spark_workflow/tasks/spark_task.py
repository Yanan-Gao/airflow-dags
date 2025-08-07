import abc
from typing import Optional, List, Tuple, Union

from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.openlineage import OpenlineageConfig


class SparkTask(abc.ABC):
    task_name: str
    executable_location: str
    class_name: Optional[str] = None
    additional_command_line_arguments: Optional[List[str]] = None
    eldorado_run_level_option_pairs_list: Optional[List[Tuple[str, str]]] = None
    do_xcom_push: bool = False

    # Set up depending on other tasks, so that we can do intra task dependencies
    depends_on: List["SparkTask"]
    openlineage_config: OpenlineageConfig

    @abc.abstractmethod
    def as_databricks(self, whl_paths: Optional[List[str]], jar_paths: Optional[List[str]]) -> DatabricksJobTask:
        pass

    @abc.abstractmethod
    def as_emr(
        self,
        spark_configurations: Optional[List[Tuple[str, str]]] = None,
        cluster_level_eldorado_configurations: Optional[List[Tuple[str, str]]] = None,
        jar_paths: Optional[List[str]] = None,
    ) -> EmrJobTask:
        pass

    def __rshift__(self, other: Union['SparkTask', List['SparkTask']]) -> Union['SparkTask', List['SparkTask']]:
        if isinstance(other, SparkTask):
            other.depends_on.append(self)

        if isinstance(other, list):
            for item in other:
                self >> item

        return other
