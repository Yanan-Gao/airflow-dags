import unittest
from unittest.mock import MagicMock, patch
from airflow.utils import timezone
from ttd.mixins.retry_mixin import RetryLimitException
from ttd.operators.ttd_emr_create_job_flow_operator import TtdEmrCreateJobFlowOperator
from ttd.ttdenv import TestEnv
from airflow.models import DAG
from botocore.exceptions import ClientError

TASK_ID = "test_task"

TEST_DAG_ID = "test_dag_id"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

JOB_FLOW_ID = "j-8989898989"
RUN_JOB_FLOW_SUCCESS_RETURN = {"ResponseMetadata": {"HTTPStatusCode": 200}, "JobFlowId": JOB_FLOW_ID}
RUN_JOB_FLOW_ERROR_THROTTLE = {"Error": {"Code": "ThrottlingException"}}
RUN_JOB_FLOW_ERROR_REQUEST_LIMIT = {"Error": {"Code": "RequestLimitExceeded"}}

DESCRIBE_CLUSTER_ERROR = {
    "Error": {
        "Code": "ThrottlingException",
        "Message":
        "An error occurred (ThrottlingException) when calling the DescribeCluster operation (reached max retries: 4): Rate exceeded"
    }
}


class TestTtdEmrcreateJobFlowOperator(unittest.TestCase):

    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dummy_dag = DAG(
            TEST_DAG_ID,
            schedule=None,
            default_args=args,
        )
        self.operator = TtdEmrCreateJobFlowOperator(
            environment=TestEnv,
            task_id=TASK_ID,
            aws_conn_id="aws_default",
            emr_conn_id="emr_default",
            region_name="ap-southeast-2",
            dag=dummy_dag
        )

        self.operator.job_flow_overrides = {"Instances": {"Ec2SubnetIds": MagicMock()}}

        self.mock_context = MagicMock()
        self.mock_task = MagicMock()
        self.mock_dag = MagicMock()
        self.mock_dag.dag_id = TEST_DAG_ID
        d = {'dag': self.mock_dag, 'task': self.mock_task}
        self.mock_context.__getitem__.side_effect = d.__getitem__

        self.operator.retry_interval = 1
        self.max_tries = 2

    def test_init(self):
        assert self.operator.aws_conn_id == "aws_default"
        assert self.operator.emr_conn_id == "emr_default"
        assert self.operator.region_name == "ap-southeast-2"

    @patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_execute_returns_job_id(self, mocked_hook_client):
        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN
        assert self.operator.execute(self.mock_context) == JOB_FLOW_ID

    @patch("airflow.providers.amazon.aws.operators.emr.get_log_uri")
    @patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_execute_returns_job_id_when_describe_cluster_fails(self, mocked_hook_client, log_uri):
        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN
        log_uri.side_effect = ClientError(operation_name='DescribeCluster', error_response=DESCRIBE_CLUSTER_ERROR)
        assert self.operator.execute(self.mock_context) == JOB_FLOW_ID

    @patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_execute_retries_with_backoff_on_throttle_succeeds_after_one_failure(self, mocked_hook_client):
        mocked_hook_client.run_job_flow.side_effect = [
            ClientError(operation_name='RunJobFlow', error_response=RUN_JOB_FLOW_ERROR_THROTTLE), RUN_JOB_FLOW_SUCCESS_RETURN
        ]
        assert self.operator.execute(self.mock_context) == JOB_FLOW_ID

    @patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_execute_retries_with_backoff_on_throttle(self, mocked_hook_client):
        mocked_hook_client.run_job_flow.side_effect = ClientError(operation_name='RunJobFlow', error_response=RUN_JOB_FLOW_ERROR_THROTTLE)
        self.assertRaises(RetryLimitException, lambda: self.operator.execute(self.mock_context))

    @patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_execute_retries_with_backoff_on_request_limit(self, mocked_hook_client):
        mocked_hook_client.run_job_flow.side_effect = ClientError(
            operation_name='RunJobFlow', error_response=RUN_JOB_FLOW_ERROR_REQUEST_LIMIT
        )
        self.assertRaises(RetryLimitException, lambda: self.operator.execute(self.mock_context))
