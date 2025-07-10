import json
from typing import Dict, Any, List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from ttd.hdinsight.hdi_livy_hook import HDILivyHook


class LivySparkSubmitOperator(BaseOperator):
    """
    An operator which submits a spark job to a HDInsight cluster using Apache Livy REST API.

    @param cluster_name: The name of the HDInsight cluster where the job gets processed.
    @type cluster_name: str
    @param jar_loc: File path for the application to be executed
    @type jar_loc: str
    @param job_class: Spark class to be executed
    @type job_class: str
    @param config_option: Spark configuration properties
    @type config_option: Dict[str, Any]
    @param driver_memory: Amount of memory to use for the driver process
    @type driver_memory: str
    @param driver_cores: Number of cores to use for the driver process
    @type driver_cores: int
    @param executor_memory: Amount of memory to use per executor process
    @type executor_memory: str
    @param executor_cores: Number of cores to use for each executor
    @type executor_cores: int
    @param command_line_arguments: Command line arguments for the application
    @type command_line_arguments: List[str]
    """

    STEP_NAME = "add_task"

    template_fields = ["cluster_name", "config_option", "command_line_args"]

    def __init__(
        self, cluster_name: str, livy_conn_id: str, jar_loc: str, job_class: Optional[str], config_option: Dict[str, Any],
        driver_memory: str, driver_cores: int, executor_memory: str, executor_cores: int, command_line_arguments: List[str], *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.cluster_name = cluster_name
        self.livy_conn_id = livy_conn_id
        self.jar_loc = jar_loc
        self.job_class = job_class
        self.config_option = config_option
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.command_line_args = command_line_arguments

    def execute(self, context: Dict[str, Any]) -> int:  # type: ignore
        from ttd.hdinsight.hdi_metrics import send_hdi_metrics

        livy_hook = HDILivyHook(cluster_name=self.cluster_name, livy_conn_id=self.livy_conn_id)
        self.log.info("Executing spark submit operator")
        try:
            batch_id = livy_hook.submit_spark_job(
                jar_loc=self.jar_loc,
                job_class=self.job_class,
                config_option=self.config_option,
                executor_memory=self.executor_memory,
                executor_cores=self.executor_cores,
                driver_cores=self.driver_cores,
                driver_memory=self.driver_memory,
                command_line_arguments=self.command_line_args,
            )
            self.log.info("Sending success execution metrics")
            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
            )
            self.log.info("Finished executing spark submit operator")
            return batch_id
        except AirflowException as e:
            json_error = json.loads(str(e))
            self.log.info("Sending fail execution metrics")
            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
                error_code=json_error["Code"],
                error_message=json_error["Message"],
            )
            raise
