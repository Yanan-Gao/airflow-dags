from functools import cache
from typing import List, Union, Optional, Dict

from ttd.eldorado.databricks.task_config import DatabricksTaskConfig, RunCondition, PythonWheelTaskLocation
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask


class PythonWheelDatabricksTask(DatabricksJobTask):

    def __init__(
        self,
        package_name: str,
        entry_point: str,
        parameters: Union[List[str], object],
        job_name: str,
        dependent_jar_s3_paths: Optional[List[str]] = None,
        whl_paths: Optional[List[str]] = None,
        depends_on: Optional[List[DatabricksJobTask]] = None,
        do_xcom_push: bool = False,
    ):
        self.name = job_name
        self.task_name = f"{self.name}_python"
        self.package_name = package_name
        self.entry_point = entry_point
        self.dependent_jar_s3_paths = dependent_jar_s3_paths
        self.parameters = parameters
        self.depends_on = depends_on if depends_on is not None else []
        self.whl_paths = whl_paths
        self.do_xcom_push = do_xcom_push

    def task_parameters(self) -> List[str]:
        return self.parameters

    @cache
    def get_main_task(self, cluster_key: str) -> DatabricksTaskConfig:
        other_jar_libs = self.dependent_jar_s3_paths if self.dependent_jar_s3_paths else []
        other_whl_libs = self.whl_paths if self.whl_paths else []
        return DatabricksTaskConfig(
            task_key=self.task_name,
            run_if=RunCondition.ALL_SUCCESS,
            task_location=PythonWheelTaskLocation(self.package_name, self.entry_point, self._expand_parameters()),
            cluster_key=cluster_key,
            other_jar_s3_paths=other_jar_libs,
            whl_paths=other_whl_libs,
            depends_on=set([x.get_main_task(cluster_key).task_key for x in self.depends_on])
        )

    def configure_openlineage(
        self, cluster_key: str, cluster_name: str, tasks: List[DatabricksTaskConfig], init_scripts: List[Dict[str, Dict[str, str]]],
        spark_configs: Dict[str, str]
    ):
        pass
