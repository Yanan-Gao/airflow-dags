from typing import Dict, Any, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from ttd.alicloud.ali_livy_hook import AliLivyHook
from ttd.alicloud.ali_hook import Defaults
from ttd.workers.worker import Workers
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig


class AliLivySensor(BaseSensorOperator):
    """
    A sensor operator for monitoring the status of a batch job using Apache Livy REST API.

    @param cluster_id: The id of the cluster where the job is being computed
    @type cluster_id: str

    @param poke_interval: Time in seconds that the sensor should wait in between each tries
    @type poke_interval: int
    @param batch_id: The batch id to be monitored
    @type batch_id: int
    """

    template_fields = ["cluster_id", "batch_id"]

    def __init__(
        self,
        cluster_id: str,
        batch_id: int,
        region: Optional[str] = Defaults.CN4_REGION_ID,
        *args,
        **kwargs,
    ):
        super().__init__(
            queue=Workers.k8s.queue,
            pool=Workers.k8s.pool,
            executor_config=K8sExecutorConfig.watch_task(),
            *args,
            **kwargs,
        )
        self.cluster_id = cluster_id
        self.batch_id = batch_id
        self.region_id = region

    def poke(self, context: Dict[str, Any]) -> bool:  # type: ignore
        livy_hook = AliLivyHook(cluster_id=self.cluster_id, region_id=self.region_id)
        state, app_id = livy_hook.get_spark_job_status_and_app_id(batch_id=self.batch_id)

        self.log.info(f"get_spark_job_status: batch_id = {self.batch_id}, state =  {state}, app_id = {app_id}")
        if state == "success":
            self._log_spark_job_logs(livy_hook)
            return True

        if state == "error" or state == "dead" or state == "killed":
            self._log_spark_job_logs(livy_hook)
            raise AirflowException("The job run was not successful.")

        return False

    def _log_spark_job_logs(self, livy_hook: AliLivyHook) -> None:
        try:
            logs = livy_hook.get_spark_job_logs(batch_id=self.batch_id)
            self.log.info("::group::Spark job logs:")
            self.log.info("\n".join(logs))
            self.log.info("::endgroup::")
        except Exception as e:
            self.log.error(f"Error getting spark job logs: {e}")
