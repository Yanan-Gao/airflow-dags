from functools import cache
from typing import List, Optional, Dict

from ttd.constants import ClusterDurations
from ttd.eldorado.databricks.task_config import DatabricksTaskConfig, RunCondition, SparkPythonTaskLocation
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask


class PythonDatabricksTask(DatabricksJobTask):
    PYTHON_ENTRYPOINT_LOCATION = "s3://ttd-dataplatform-infra-useast/databricks/entrypoint.py"

    def __init__(
        self,
        entrypoint_path: str,
        args: List[str],
        job_name: str,
        dependent_jar_s3_paths: Optional[List[str]] = None,
        whl_paths: Optional[List[str]] = None,
        depends_on: Optional[List[DatabricksJobTask]] = None,
        do_xcom_push: bool = False,
        timeout_seconds: int = ClusterDurations.DEFAULT_MAX_DURATION,
    ):
        self.name = job_name
        self.task_name = f"{self.name}_python"
        self.entrypoint_path = entrypoint_path
        self.dependent_jar_s3_paths = dependent_jar_s3_paths
        self.whl_paths = whl_paths
        self.depends_on = depends_on if depends_on else []
        self.do_xcom_push = do_xcom_push
        self.timeout_seconds = timeout_seconds

        if not self.entrypoint_path.startswith("/"):
            self.entrypoint_path = "/" + self.entrypoint_path
        self.args = args

        self.parameters = [self.entrypoint_path] + self.args

    def task_parameters(self) -> List[str]:
        return self.parameters

    @cache
    def get_main_task(self, cluster_key: str) -> DatabricksTaskConfig:
        other_jar_libs = self.dependent_jar_s3_paths if self.dependent_jar_s3_paths else []
        return DatabricksTaskConfig(
            task_key=self.task_name,
            run_if=RunCondition.ALL_SUCCESS,
            task_location=SparkPythonTaskLocation(s3_path=self.PYTHON_ENTRYPOINT_LOCATION, params=self._expand_parameters()),
            cluster_key=cluster_key,
            other_jar_s3_paths=other_jar_libs,
            whl_paths=self.whl_paths,
            depends_on=set([x.get_main_task(cluster_key).task_key for x in self.depends_on]),
            timeout_seconds=self.timeout_seconds,
        )

    def configure_openlineage(
        self, cluster_key: str, cluster_name: str, tasks: List[DatabricksTaskConfig], init_scripts: List[Dict[str, Dict[str, str]]],
        spark_configs: Dict[str, str]
    ):
        pass
