from typing import Optional, Dict, Any, List, Tuple

from ttd.constants import ClusterDurations
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.eldorado.databricks.tasks.notebook_databricks_task import NotebookDatabricksTask
from ttd.openlineage import OpenlineageConfig
from ttd.spark import SparkTask


class NotebookTask(SparkTask):
    """
    Defines a notebook task to run agnostically on a variety of backends.
    This expects that the notebook is loaded onto workspace.
    """

    def __init__(
        self,
        task_name: str,
        notebook_path: str,
        notebook_params: Optional[Dict[str, Any]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        do_xcom_push: bool = False,
        timeout_seconds: int = ClusterDurations.DEFAULT_MAX_DURATION,
    ) -> None:

        self.task_name = task_name
        self.executable_location = notebook_path
        self.notebook_params = notebook_params
        self.openlineage_config = openlineage_config
        self.do_xcom_push = do_xcom_push
        self.depends_on: list[SparkTask] = []
        self.timeout_seconds = timeout_seconds

    def as_databricks(self, whl_paths: Optional[List[str]], jar_paths: Optional[List[str]]) -> DatabricksJobTask:
        return NotebookDatabricksTask(
            notebook_path=self.executable_location,
            job_name=self.task_name,
            notebook_params=self.notebook_params,
            dependent_jar_s3_paths=jar_paths,
            whl_paths=whl_paths,
            openlineage_config=self.openlineage_config,
            do_xcom_push=self.do_xcom_push,
            timeout_seconds=self.timeout_seconds,
        )

    def as_emr(
        self,
        spark_configurations: Optional[List[Tuple[str, str]]] = None,
        cluster_level_eldorado_configurations: Optional[List[Tuple[str, str]]] = None,
        jar_paths: Optional[List[str]] = None
    ) -> EmrJobTask:
        raise NotImplementedError
