import ast
import logging
from typing import Any, Dict, Optional

from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.links.emr import EmrClusterLink
from airflow.utils.context import Context

from ttd.aws.emr.cluster_clone_link import ClusterCloneLink
from ttd.aws.emr.cluster_logs_link import ClusterLogsLink, EmrLogsLink
from ttd.eldorado.aws.emr_cluster_specs import get_emr_cluster_roles
from ttd.metrics.cluster import ClusterLifecycleMetricPusher, ClusterTaskData
from ttd.ttdenv import TtdEnv
from ttd.mixins.retry_mixin import RetryMixin
from botocore.exceptions import ClientError
import time
import random


class TtdEmrCreateJobFlowOperator(RetryMixin, EmrCreateJobFlowOperator):

    operator_extra_links = (EmrClusterLink(), EmrLogsLink(), ClusterCloneLink(), ClusterLogsLink())  # type: ignore

    def __init__(self, region_name: str, environment: TtdEnv, cluster_task_data: Optional[ClusterTaskData] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_name = region_name
        self.environment = environment
        self.cluster_task_data = cluster_task_data
        self.job_flow_id = None

    def execute(self, context: Dict[str, Any]):  # type: ignore
        emr_compute_role, emr_service_role = get_emr_cluster_roles(
            region=self.region_name,
            dag_id=context["dag"].dag_id,
            team=context["task"].owner,
            environment=self.environment,
        )
        self.job_flow_overrides["JobFlowRole"] = emr_compute_role  # type: ignore
        self.job_flow_overrides["ServiceRole"] = emr_service_role  # type: ignore

        subnet_ids = self.job_flow_overrides["Instances"]["Ec2SubnetIds"]  # type: ignore
        if isinstance(subnet_ids, str):
            self.job_flow_overrides["Instances"]["Ec2SubnetIds"] = ast.literal_eval(subnet_ids)  # type: ignore

        logging.info(f"Job flow config: {self.job_flow_overrides}")
        # Attempt to move cluster creation around by a few seconds, so that the requests fall
        # into different buckets to reduce throttling.
        jitter_amount = random.random() * 4
        logging.info(f"Introducing scheduling jitter: {jitter_amount}")
        time.sleep(jitter_amount)

        job_flow_id = self.with_retry(
            lambda: super(TtdEmrCreateJobFlowOperator, self).execute(context),  # type: ignore
            lambda ex: isinstance(ex, ClientError) and self._should_keep_waiting(ex.response)
        )

        if job_flow_id.is_success:
            self.job_flow_id = job_flow_id.get()
            return self.job_flow_id
        else:
            return self.job_flow_id if self.job_flow_id else job_flow_id.get()

    def _should_keep_waiting(self, response: Dict) -> bool:
        error_code = response.get("Error", {}).get('Code') if response else None
        message = response.get("Error", {}).get('Message') if response else None
        if error_code == 'ThrottlingException' or error_code == 'RequestLimitExceeded':
            # Note: https://github.com/apache/airflow/blob/main/providers/src/airflow/providers/amazon/aws/operators/emr.py#L721
            # Its quite possible that we get to this point, and the EMR cluster has actually succeeded in being created, and
            # we're failing because of the subsequent throttling on the describe cluster operation to get the log messages.
            # In this case, we should continue since we don't want to create the cluster twice.
            if message is not None and 'DescribeCluster' in message and self._job_flow_id:
                self.job_flow_id = self._job_flow_id
                self.log.info("The describe cluster call was throttled by AWS, but the create cluster operation has succeeded")
                return False

            self.log.info("Throttled by AWS. Keep waiting.")
            return True

        return False

    def post_execute(self, context: Context, result=None):
        if self.cluster_task_data is not None:
            ClusterLifecycleMetricPusher().cluster_startup_requested(self.job_flow_id, context, self.cluster_task_data)
