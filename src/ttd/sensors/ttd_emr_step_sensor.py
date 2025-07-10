import math
import time
from random import random

from airflow.utils import timezone
from airflow.utils.context import Context
import botocore
from airflow.providers.amazon.aws.operators.emr import EmrHook
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.links.emr import (
    EmrClusterLink,
    get_log_uri,
)
from ttd.aws.emr.cluster_logs_link import EmrLogsLink
from ttd.workers.worker import Workers
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.monads.trye import Try


class TtdEmrStepSensor(EmrStepSensor):
    """
    Asks for the state of the step until it reaches a terminal state.
    If it fails the sensor errors, failing the task.
    Uses exponential back-off if case of 'ThrottlingException' from AWS API

    :param job_flow_id: job_flow_id which contains the step check the state of
    :type job_flow_id: str
    :param step_id: step to check the state of
    :type step_id: str
    """

    def __init__(
        self,
        job_flow_id: str,
        step_id: str,
        cluster_task_id: str,
        max_emr_response_retries: int = 5,
        backoff_exp: int = 2,
        region_name: str = None,
        aws_conn_id="aws_default",
        *args,
        **kwargs,
    ):
        super(TtdEmrStepSensor, self).__init__(
            job_flow_id=job_flow_id,
            step_id=step_id,
            queue=Workers.k8s.queue,
            pool=Workers.k8s.pool,
            executor_config=K8sExecutorConfig.watch_task(),
            *args,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )
        self.cluster_task_id = cluster_task_id
        self.backoff_exp = backoff_exp
        self.max_emr_response_retries = max_emr_response_retries
        self.region_name = region_name

    def get_emr_response(self, context: Context):
        started_at = timezone.utcnow()
        attempt_number = 0
        while True:
            attempt_number += 1

            # if no region_name was passed in (i.e. we're running in us-east-1) the base functionality is fine.
            # Otherwise, we need custom logic
            if self.region_name is None:
                attempt = Try.apply(lambda: super(TtdEmrStepSensor, self).get_emr_response(context))  # type: ignore
            else:
                attempt = Try.apply(lambda: self.get_emr_response_with_region(context))

            if attempt.is_success:
                return attempt.get()

            ex = attempt.failed().get()
            self.log.debug("Error: " + str(ex))
            if (isinstance(ex, botocore.exceptions.ClientError)
                    and ex.response.get("Error", {}).get("Code", "Unknown") == "ThrottlingException"):
                sleep_time = self.calc_sleep(attempt_number, self.backoff_exp)
                if (attempt_number < self.max_emr_response_retries
                        and (timezone.utcnow() - started_at).total_seconds() + sleep_time < self.poke_interval and not self.reschedule):
                    self.log.info(
                        f"ThrottlingException on EMR DescribeStep attempt {attempt_number + 1} out of {self.max_emr_response_retries}, retrying"
                    )
                    time.sleep(sleep_time)
                else:
                    self.log.error("ThrottlingException on EMR DescribeStep attempt beyond limits, returning empty result to reschedule.")
                    return {"ResponseMetadata": {"HTTPStatusCode": 499}}
            else:
                attempt.get()

    def get_emr_response_with_region(self, context: Context):
        """
        This is a blatant copy-paste of the base class get_emr_response(self) function, but with region_name added
        to the EmrHook. Why? Because the API is inconsistent and adds unnecessary complexity. The CreateJobFlow operator
        takes in region_name as an argument and works just fine for starting a cluster in the specified AWS region.
        But none of the other EMR operators or sensors take in that argument, despite the fact they all COULD because
        they all still just wrap around EmrHook. Instead, they want you to do this weird thing with defining alternate
        aws connection IDs, but none of the docs actually explain how to do this clearly. So this is an attempt to
        address this weird gap in the API for what should be very basic functionality
        """
        emr = EmrHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name).get_conn()

        self.log.info("Poking step %s on cluster %s", self.step_id, self.job_flow_id)
        response = emr.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=self.region_name,
            aws_partition=self.hook.conn_partition,
            job_flow_id=self.job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=self.region_name,
            aws_partition=self.hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(emr_client=emr, job_flow_id=self.job_flow_id),
        )

        return response

    @staticmethod
    def calc_sleep(attempt_number: int, backoff_exp: int) -> int:
        next_interval = backoff_exp**attempt_number - backoff_exp**(attempt_number - 1)
        random_interval = math.ceil(next_interval * random())
        return backoff_exp**(attempt_number - 1) + random_interval
