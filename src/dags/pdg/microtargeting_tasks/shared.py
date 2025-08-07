"""
Shared functionality for AWS and Azure Microtargeting tasks
"""
from collections import namedtuple
from datetime import timedelta, datetime
from typing import List, Tuple

from airflow.models import TaskInstance
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator

from dags.pdg.task_utils import choose, xcom_pull_str
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import pdg
from ttd.tasks.op import OpTask


class MicrotargetingTasksShared:
    CONTEXT_TASK_ID = "set-context-shared"

    KEY_S3_GEO_BUCKET_NAME = "s3_geo_bucket_name"
    KEY_S3_GEO_PREFIX = "s3_geo_prefix_key"

    AWS_CONNECTION_ID = "ttd-microtargeting-job-aws"
    AZURE_CONNECTION_ID = "ttd-microtargeting-job-azure"

    PROV_PROD_AND_TEST_SECRETS = namedtuple('PROV_PROD_AND_TEST_SECRETS', ['prod', 'test'])

    def __init__(self, dag_id: str, job_name: str):
        self.dag_id = dag_id
        self.job_name = job_name

    def prov_params(self, access_mode: str, prod_and_test_secrets: PROV_PROD_AND_TEST_SECRETS) -> List[Tuple[str, str]]:
        prov_db = "{{{{ '{prod}' if params.prov_db == 'prod' else '{test}' }}}}"

        prov_host = (
            prov_db.format(prod="provdb.adsrvr.org", test="dip-provdb.adsrvr.org")
            if access_mode == 'read-write' else prov_db.format(prod="provdb-bi.adsrvr.org", test="dip-provdb.adsrvr.org")
        )

        return [("provisioningHost", prov_host),
                ("provisioningUserPassSecretName", prov_db.format(prod=prod_and_test_secrets.prod, test=prod_and_test_secrets.test))]

    PROV_MODE_PARAMS: List[Tuple[str, str]] = [
        ("provMode", "{{ params.prov_mode }}"),
    ]

    POLICY_PARAMS: List[Tuple[str, str]] = [
        ("policyLabel", "{{ params.policy_label }}"),
        ("policyId", "{{ params.policy_id }}"),
    ]

    def get_shared_context_value(self, key: str):
        return xcom_pull_str(dag_id=self.dag_id, task_id=MicrotargetingTasksShared.CONTEXT_TASK_ID, key=key)

    def create_dag(
        self, additional_tags: List[str], start_date: datetime, schedule_interval: str, retries: int, dagrun_timeout: timedelta,
        policy_label: str, policy_id: int
    ):
        return TtdDag(
            dag_id=self.dag_id,
            slack_channel='#scrum-pdg-alerts',
            tags=[pdg.name, pdg.jira_team] + additional_tags,
            start_date=start_date,
            schedule_interval=schedule_interval,
            retries=retries,
            params={
                "prov_db": Param(choose(prod="prod", test="internal"), enum=["internal", "prod"]),
                "prov_mode": Param("read-write", enum=["read-only", "read-write"]),
                "policy_label": policy_label,
                "policy_id": policy_id,
            },
            dagrun_timeout=dagrun_timeout,
            run_only_latest=True,  # For MT, missed runs can't be made up
        )

    def _set_shared_context(self, task_instance: TaskInstance, **kwargs):
        geo_bucket_name = "ttd-geo"
        geo_prefix_key = "env=prod"  # test is not kept up to date so we should use prod data always
        task_instance.xcom_push(key=MicrotargetingTasksShared.KEY_S3_GEO_BUCKET_NAME, value=geo_bucket_name)
        task_instance.xcom_push(key=MicrotargetingTasksShared.KEY_S3_GEO_PREFIX, value=geo_prefix_key)

    def create_shared_set_context_task(self, airflow_dag):
        return OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksShared.CONTEXT_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._set_shared_context,
                retry_exponential_backoff=True
            )
        )
