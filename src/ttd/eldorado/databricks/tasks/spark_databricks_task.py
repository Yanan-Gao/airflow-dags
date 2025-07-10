from typing import Optional, List, Tuple, Dict

from ttd.cloud_provider import DatabricksCloudProvider
from ttd.eldorado.databricks.task_config import DatabricksTaskConfig, RunCondition, SparkJarTaskLocation, \
    SparkPythonTaskLocation
from functools import cache

from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask, RunLevelJvmParameterParser
from ttd.openlineage import OpenlineageConfig, OpenlineageTransport
from ttd.ttdenv import TtdEnvFactory


class SparkDatabricksTask(DatabricksJobTask):

    def __init__(
        self,
        class_name: str,
        executable_path: str,
        job_name: str,
        eldorado_run_options_list: Optional[List[Tuple[str, str]]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        depends_on: Optional[List[DatabricksJobTask]] = None,
        additional_command_line_parameters: Optional[List[str]] = None,
        run_level_arg_parser=RunLevelJvmParameterParser(),
        do_xcom_push: bool = False,
        run_condition=RunCondition.ALL_SUCCESS
    ):
        """
        Run a spark job packaged into a jar.

        Special things to note here - the `run_level_arg_parser` will parse the `eldorado_run_options_list`
        and pass the result to the task. The nuance here is that on EMR we typically pass args to the
        JVM via the extra driver options, however on Databricks you can only have one set of spark configuration
        set at the cluster level, and they cannot be overwritten on the task level.

        Because of this, special care should be taken when passing parameters that way to the job, and
        they should instead be parsed by your job specifically. The `run_level_arg_parser` will take
        any parameters specified at the task level, and format them into `foo=bar` args that get passed
        as command line arguments to your job.

        If you'd like to take those arguments and have them be parsed into the `TTDConfig` that is used
        within eldorado, you can add the following snippet to your `main` function:

        ```
        import com.thetradedesk.spark.util.TTDConfig.config

        def main(args: Array[String]): Unit = {

          args.foreach(arg => {
            val split = arg.split('=')

            config.overridePathVariable(split(0), split(1))
        })
        ```

        If you're not sure what this means, then you should pass arguments via the
        `additional_command_line_parameters` and your job will be responsible for parsing them, as usual.
        """
        self.name = job_name
        self.task_name = f"{self.name}_spark"
        self.class_name = class_name
        self.executable_path = executable_path
        self.openlineage_config = openlineage_config
        self._run_level_jar_params = eldorado_run_options_list
        self.depends_on = depends_on if depends_on else []
        self.additional_command_line_parameters = additional_command_line_parameters[:] if additional_command_line_parameters else []
        self.run_level_arg_parser = run_level_arg_parser
        self.do_xcom_push = do_xcom_push
        self.run_condition = run_condition

        formatted_run_level_args = self.run_level_arg_parser.parse(self._run_level_jar_params)
        self.parameters = formatted_run_level_args + self.additional_command_line_parameters

    def task_parameters(self) -> List[str]:
        return self.parameters

    @cache
    def get_main_task(self, cluster_key: str) -> DatabricksTaskConfig:
        return DatabricksTaskConfig(
            task_key=self.task_name,
            run_if=self.run_condition,
            task_location=
            SparkJarTaskLocation(class_name=self.class_name, jar_location=self.executable_path, parameters=self._expand_parameters()),
            cluster_key=cluster_key,
            depends_on=set([x.get_main_task(cluster_key).task_key for x in self.depends_on])
        )

    def configure_openlineage(
        self, cluster_key: str, cluster_name: str, tasks: List[DatabricksTaskConfig], init_scripts: List[Dict[str, Dict[str, str]]],
        spark_configs: Dict[str, str]
    ):
        """
        Note: This may modify the spark configs, init scripts, and tasks
        (by reference) that are passed into this function. This is due to
        needing to provide parameters to open lineage, as well as potentially
        appending a task to the cluster that will set up the task that copies
        the open lineage data to s3.
        """
        if self.openlineage_config.enabled:
            # provider
            databricks_provider = DatabricksCloudProvider()
            # Set the openlineage configs
            spark_configs.update(
                self.openlineage_config.get_java_spark_options_dict(
                    app_name=self.name,
                    class_name=self.class_name,
                    namespace=cluster_name,
                    env=TtdEnvFactory.get_from_system(),
                    provider=DatabricksCloudProvider()
                )
            )

            # Setup the initscript that installs the jar
            init_scripts.append(self.openlineage_config.assets_config.get_initscript_config())

            # Setup the shutdown script if necessary
            if self.openlineage_config.transport == OpenlineageTransport.ROBUST:
                upload_robust_lineage_task = DatabricksTaskConfig(
                    task_key=f"{self.name}_upload_lineage",
                    run_if=RunCondition.ALL_DONE,
                    task_location=SparkPythonTaskLocation(
                        s3_path=self.openlineage_config.assets_config.get_robust_upload_task_url(databricks_provider),
                        params=self.openlineage_config.get_robust_upload_params_for_java_task(
                            app_name=self.name,
                            class_name=self.class_name,
                            cloud_provider=databricks_provider,
                            env=TtdEnvFactory.get_from_system()
                        )
                    ),
                    cluster_key=cluster_key
                )
                main_task = self.get_main_task(cluster_key)
                upload_robust_lineage_task.depend_on(main_task)
                tasks.append(upload_robust_lineage_task)
