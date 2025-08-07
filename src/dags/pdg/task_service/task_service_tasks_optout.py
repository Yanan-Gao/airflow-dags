from datetime import datetime
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import pdg

from dags.pdg.data_subject_request.util import secrets_manager_util

registry = TaskServiceDagRegistry(globals())


class EuidSecrets:

    @staticmethod
    def get_secrets():
        secrets = secrets_manager_util.get_secrets(secret_name='euid-airflow', region_name='us-west-2')
        return secrets


registry.register_dag(
    TaskServiceDagFactory(
        task_name="LiveRampEuidOptOutTask",
        task_config_name="LiveRampEuidOptOutTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2023, 12, 12),
        job_schedule_interval="0 9 * * *",
        resources=TaskServicePodResources.medium(),
        # branch_name="gfl-PDG-775-liveramp-euid-optout-test",
        configuration_overrides={
            "LiveRampEuidOptOutTask.Enabled":
            "true",
            "LiveRampEuidOptOutTask.ExecutionInterval":
            "24:00:00",
            "LiveRampEuidOptOutTask.EuidEndpoint":
            "https://prod.euid.eu",
            # "LiveRampEuidOptOutTask.EuidAuthKey": EuidSecrets.get_secrets()['EUID_API_KEY'],
            # "LiveRampEuidOptOutTask.EuidSecretKey": EuidSecrets.get_secrets()['EUID_API_V2_SECRET'],
            "LiveRampEuidOptOutTask.Bucket":
            "thetradedesk-useast-data-import",
            "LiveRampEuidOptOutTask.Prefix":
            "liveramp/",
            "LiveRampEuidOptOutTask.DataServer":
            "va6-data.adsrvr.org",
            "LiveRampEuidOptOutTask.RunDateTime":
            '{{ dag_run.conf.get("RunDateTime") if dag_run.conf is not none and dag_run.conf.get("RunDateTime") is not none else logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}',
            "LiveRampEuidOptOutTask.NoExpirationTimeCheck":
            '{{ dag_run.conf.get("NoExpirationTimeCheck") if dag_run.conf is not none and dag_run.conf.get("NoExpirationTimeCheck") is not none else "false" }}'
        },
    )
)
