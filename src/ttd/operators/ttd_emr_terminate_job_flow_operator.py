from airflow.providers.amazon.aws.operators.emr import (
    EmrHook,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.links.emr import (
    EmrClusterLink,
    get_log_uri,
)
from ttd.aws.emr.cluster_logs_link import EmrLogsLink
from airflow.exceptions import AirflowException

from ttd.metrics.cluster import ClusterLifecycleMetricPusher


class TtdEmrTerminateJobFlowOperator(EmrTerminateJobFlowOperator):

    def __init__(self, job_flow_id, region_name=None, aws_conn_id="aws-default", *args, **kwargs):
        super(TtdEmrTerminateJobFlowOperator, self).__init__(job_flow_id=job_flow_id, aws_conn_id=aws_conn_id, *args, **kwargs)
        self.region_name = region_name

    def execute(self, context):
        """
        This is a blatant copy-paste of the base class execute(self, context) function, but with region_name added
        to the EmrHook. Why? Because the API is inconsistent and adds unnecessary complexity. The CreateJobFlow operator
        takes in region_name as an argument and works just fine for starting a cluster in the specified AWS region.
        But none of the other EMR operators or sensors take in that argument, despite the fact they all COULD because
        they all still just wrap around EmrHook. Instead, they want you to do this weird thing with defining alternate
        aws connection IDs, but none of the docs actually explain how to do this clearly. So this is an attempt to
        address this weird gap in the API for what should be very basic functionality
        """

        # if no region name was passed in (i.e. we're running in us-east-1) base functionality is fine
        if self.region_name is None:
            super(TtdEmrTerminateJobFlowOperator, self).execute(context)
            return

        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        emr = emr_hook.get_conn()

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=self.region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=self.region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(emr_client=emr, job_flow_id=self.job_flow_id),
        )

        self.log.info("Terminating JobFlow %s", self.job_flow_id)
        response = emr.terminate_job_flows(JobFlowIds=[self.job_flow_id])

        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException("JobFlow termination failed: %s" % response)
        else:
            self.log.info("JobFlow with id %s terminated", self.job_flow_id)
            ClusterLifecycleMetricPusher().cluster_termination_requested(self.job_flow_id, context)
