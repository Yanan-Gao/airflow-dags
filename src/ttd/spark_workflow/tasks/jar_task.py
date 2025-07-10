from typing import Optional, List, Tuple

from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.openlineage import OpenlineageConfig
from ttd.spark import SparkJobBuilderException
from ttd.spark_workflow.tasks.spark_task import SparkTask


class JarTask(SparkTask):
    """
    Defines a spak task to run agnostically on a variety of backends.
    This expects that the jar is loaded onto s3.
    """

    def __init__(
        self,
        task_name: str,
        jar_location: str,
        class_name: str,
        additional_command_line_arguments: Optional[List[str]] = None,
        eldorado_run_level_option_pairs_list: Optional[List[Tuple[str, str]]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        do_xcom_push: bool = False,
    ) -> None:

        self.task_name = task_name
        self.executable_location = jar_location
        self.class_name = class_name
        self.additional_command_line_arguments = additional_command_line_arguments
        self.eldorado_run_level_option_pairs_list = eldorado_run_level_option_pairs_list
        self.openlineage_config = openlineage_config
        self.do_xcom_push = do_xcom_push
        self.depends_on: list[SparkTask] = []

    def as_databricks(self, whl_paths: Optional[List[str]], jar_paths: Optional[List[str]]) -> DatabricksJobTask:
        if self.class_name is None:
            raise SparkJobBuilderException("Spark job was detected to be a jar, however no class name was provided")

        return SparkDatabricksTask(
            class_name=self.class_name,
            executable_path=self.executable_location,
            job_name=self.task_name,
            eldorado_run_options_list=self.eldorado_run_level_option_pairs_list,
            openlineage_config=self.openlineage_config,
            do_xcom_push=self.do_xcom_push,
        )

    def as_emr(
        self,
        spark_configurations: Optional[List[Tuple[str, str]]] = None,
        cluster_level_eldorado_configurations: Optional[List[Tuple[str, str]]] = None,
        jar_paths: Optional[List[str]] = None,
    ) -> EmrJobTask:
        if self.class_name is None:
            raise SparkJobBuilderException("Spark job was detected to be a jar, however no class name was provided")

        eldorado_configs = self.eldorado_run_level_option_pairs_list if self.eldorado_run_level_option_pairs_list else []

        if cluster_level_eldorado_configurations is None:
            cluster_level_eldorado_configurations = []

        spark_config_pairs = spark_configurations[:] if spark_configurations else []
        if jar_paths:
            spark_config_pairs.append(("jars", ",".join(jar_paths)))

        return EmrJobTask(
            name=self.task_name,
            class_name=self.class_name,
            eldorado_config_option_pairs_list=cluster_level_eldorado_configurations[:] + eldorado_configs,
            additional_args_option_pairs_list=spark_config_pairs,
            executable_path=self.executable_location,
            command_line_arguments=self.additional_command_line_arguments,
            configure_cluster_automatically=False,
            maximize_resource_allocation=True,
            openlineage_config=self.openlineage_config,
            do_xcom_push=self.do_xcom_push,
        )
