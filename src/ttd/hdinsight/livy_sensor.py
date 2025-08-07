from __future__ import annotations

import json
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from ttd.metrics.job_lifecycle import JobLifecycleMetricPusher
from ttd.workers.worker import Workers
from ttd.hdinsight.hdi_livy_hook import HDILivyHook
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig


class LivySensor(BaseSensorOperator):
    """
    A sensor operator for monitoring the status of a batch job using Apache Livy REST API.

    @param cluster_name: The name of the cluster where the job is being computed
    @type cluster_name: str
    @param poke_interval: Time in seconds that the sensor should wait in between each tries
    @type poke_interval: int
    @param batch_id: The batch id to be monitored
    @type batch_id: int
    """

    STEP_NAME = "watch_task"

    template_fields = ["cluster_name", "batch_id"]

    def __init__(self, cluster_name: str, batch_id: int | str, livy_conn_id: str, *args, **kwargs):
        super().__init__(
            queue=Workers.k8s.queue,
            pool=Workers.k8s.pool,
            executor_config=K8sExecutorConfig.watch_task(),
            *args,
            **kwargs,
        )
        self.cluster_name = cluster_name
        self.batch_id = batch_id
        self.livy_conn_id = livy_conn_id

    def poke(self, context: Context) -> bool:  # type: ignore
        from ttd.hdinsight.hdi_metrics import send_hdi_metrics

        livy_hook = HDILivyHook(cluster_name=self.cluster_name, livy_conn_id=self.livy_conn_id)
        try:
            state, app_id = livy_hook.get_spark_job_status_and_app_id(batch_id=self.batch_id)
        except AirflowException as e:
            self.log.info("Sending fail execution metrics")
            json_error = json.loads(str(e))
            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
                error_code=json_error["Code"],
                error_message=json_error["Message"],
            )
            raise

        context['task_instance'].xcom_push(key='app_id', value=app_id)

        if state == "success":
            self._log_spark_job_logs(livy_hook)
            return True

        if state == "error" or state == "dead" or state == "killed":
            self._log_spark_job_logs(livy_hook)

            JobLifecycleMetricPusher().job_terminated(
                self.cluster_name,
                context["run_id"],
                self.batch_id,
                success=False,
                fail_state=state,
            )

            raise AirflowException("The job run was not successful.")

        return False

    def post_execute(self, context: Context, result: Any = None) -> None:
        from ttd.hdinsight.hdi_metrics import send_hdi_metrics

        self.log.info("Sending success execution metrics")
        send_hdi_metrics(
            operator=self,
            step_name=self.STEP_NAME,
            context=context,
        )

        JobLifecycleMetricPusher().job_terminated(
            self.cluster_name,
            context["run_id"],
            self.batch_id,
            success=True,
        )

    def _log_spark_job_logs(self, livy_hook: HDILivyHook) -> None:
        try:
            logs = livy_hook.get_spark_job_logs(batch_id=self.batch_id)
            self.log.info("::group::Spark job logs:")
            self.log.info("\n".join(logs))
            self.log.info("::endgroup::")
        except Exception as e:
            self.log.error(f"Error getting spark job logs: {e}")
