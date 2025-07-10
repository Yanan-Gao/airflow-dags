from functools import cache
from typing import Optional, Dict, Any, List

from ttd.eldorado.databricks.task_config import DatabricksTaskConfig, RunCondition, SparkNotebookTaskLocation
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.openlineage import OpenlineageConfig


class NotebookDatabricksTask(DatabricksJobTask):

    def __init__(
        self,
        job_name: str,
        notebook_path: str,
        notebook_params: Optional[Dict[str, Any]] = None,
        dependent_jar_s3_paths: Optional[List[str]] = None,
        whl_paths: Optional[List[str]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        depends_on: Optional[List[DatabricksJobTask]] = None,
        do_xcom_push: bool = False,
    ):
        self.name = job_name
        self.task_name = f"{self.name}_notebook"
        self.notebook_path = notebook_path
        self.notebook_params = notebook_params if notebook_params is not None else {}
        self.dependent_jar_s3_paths = dependent_jar_s3_paths
        self.whl_paths = whl_paths
        self.openlineage_config = openlineage_config
        self.depends_on = depends_on if depends_on else []
        self.do_xcom_push = do_xcom_push

    def task_parameters(self) -> List[str]:
        return list()

    @cache
    def get_main_task(self, cluster_key: str) -> DatabricksTaskConfig:
        return DatabricksTaskConfig(
            task_key=self.task_name,
            run_if=RunCondition.ALL_SUCCESS,
            task_location=SparkNotebookTaskLocation(notebook_path=self.notebook_path, base_parameters=self.notebook_params),
            cluster_key=cluster_key,
            other_jar_s3_paths=self.dependent_jar_s3_paths,
            whl_paths=self.whl_paths,
            depends_on=set([x.get_main_task(cluster_key).task_key for x in self.depends_on])
        )

    def configure_openlineage(
        self, cluster_key: str, cluster_name: str, tasks: List[DatabricksTaskConfig], init_scripts: List[Dict[str, Dict[str, str]]],
        spark_configs: Dict[str, str]
    ):
        pass
