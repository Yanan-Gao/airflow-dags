from typing import Dict, Any, List, Optional

from airflow.models import BaseOperator

from ttd.alicloud.ali_hook import Defaults
from ttd.alicloud.ali_livy_hook import AliLivyHook


class AliSparkSubmitOperator(BaseOperator):
    template_fields = ["cluster_id", "command_line_arguments", "config_option"]

    def __init__(
        self,
        cluster_id: str,
        jar_loc: str,
        job_class: str,
        config_option: Dict[str, Any],
        driver_memory: str,
        driver_cores: int,
        executor_memory: str,
        executor_cores: int,
        command_line_arguments: List[str],
        region: Optional[str] = Defaults.CN4_REGION_ID,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.jar_loc = jar_loc
        self.job_class = job_class
        self.config_option = config_option
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.command_line_arguments = command_line_arguments
        self.region_id = region

    def execute(self, context: Dict[str, Any]) -> int:  # type: ignore
        livy_hook = AliLivyHook(region_id=self.region_id, cluster_id=self.cluster_id)
        self.log.info(
            f"spark conf: driver_memory={self.driver_memory}, "
            f"driver_cores={self.driver_cores}, executor_memory= {self.executor_memory}, "
            f"executor_cores={self.executor_cores}, command_line_arguments= {self.command_line_arguments},  "
            f"config_option= {self.config_option}"
        )
        self.log.info("Executing spark submit operator")
        batch_id = livy_hook.submit_spark_job(
            jar_loc=self.jar_loc,
            job_class=self.job_class,
            config_option=self.config_option,
            executor_memory=self.executor_memory,
            executor_cores=self.executor_cores,
            driver_cores=self.driver_cores,
            driver_memory=self.driver_memory,
            command_line_arguments=self.command_line_arguments,
        )
        self.log.info("Finished executing spark submit operator")

        return batch_id
