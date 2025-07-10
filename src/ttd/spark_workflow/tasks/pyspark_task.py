from typing import Optional, List, Tuple

from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.aws.emr_pyspark import S3PysparkEmrTask
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.openlineage import OpenlineageConfig
from ttd.spark_workflow.tasks.spark_task import SparkTask


class PySparkTask(SparkTask):
    """
    Defines a pyspark task to run agnostically on a variety of backends.
    This expects that the entrypoint is located on s3, and that the backends
    have the permissions necessary to download that entrypoint.
    """

    def __init__(
        self,
        task_name: str,
        python_entrypoint_location: str,
        additional_command_line_arguments: Optional[List[str]] = None,
        eldorado_run_level_option_pairs_list: Optional[List[Tuple[str, str]]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        do_xcom_push: bool = False,
    ) -> None:

        self.task_name = task_name
        self.executable_location = python_entrypoint_location
        self.additional_command_line_arguments = additional_command_line_arguments
        self.eldorado_run_level_option_pairs_list = eldorado_run_level_option_pairs_list
        self.openlineage_config = openlineage_config
        self.do_xcom_push = do_xcom_push
        self.depends_on: list[SparkTask] = []

    def as_databricks(self, whl_paths: Optional[List[str]], jar_paths: Optional[List[str]]) -> DatabricksJobTask:
        return S3PythonDatabricksTask(
            entrypoint_path=self.executable_location,
            job_name=self.task_name,
            args=self.additional_command_line_arguments if self.additional_command_line_arguments is not None else [],
            whl_paths=whl_paths,
            dependent_jar_s3_paths=jar_paths,
            do_xcom_push=self.do_xcom_push,
        )

    def as_emr(
        self,
        spark_configurations: Optional[List[Tuple[str, str]]] = None,
        cluster_level_eldorado_configurations: Optional[List[Tuple[str, str]]] = None,
        jar_paths: Optional[List[str]] = None,
    ) -> EmrJobTask:
        eldorado_configs = self.eldorado_run_level_option_pairs_list if self.eldorado_run_level_option_pairs_list else []

        if cluster_level_eldorado_configurations is None:
            cluster_level_eldorado_configurations = []

        spark_config_pairs = spark_configurations[:] if spark_configurations else []
        if jar_paths:
            spark_config_pairs.append(("jars", ",".join(jar_paths)))

        return S3PysparkEmrTask(
            name=self.task_name,
            entry_point_path=self.executable_location,
            eldorado_config_option_pairs_list=cluster_level_eldorado_configurations[:] + eldorado_configs,
            additional_args_option_pairs_list=spark_config_pairs,
            command_line_arguments=self.additional_command_line_arguments,
            configure_cluster_automatically=False,
            maximize_resource_allocation=True,
            do_xcom_push=self.do_xcom_push,
        )
