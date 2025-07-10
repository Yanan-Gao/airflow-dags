from datetime import datetime, timedelta

# Required comment
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import dist
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

team_slack_channel = "#tf-rates-and-fees-alerts"

registry = TaskServiceDagRegistry(globals())

validation_task_max_retries = 4
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataRatesCDCValidationTask",
        task_name_suffix="partition-1",
        task_config_name="ThirdPartyDataRatesCDCValidationConfig",
        scrum_team=dist,
        job_schedule_interval="0 12 * * *",  # Runs daily at 12:00 UTC time.
        retries=validation_task_max_retries,
        retry_delay=timedelta(minutes=30),
        start_date=datetime.now(),
        run_only_latest=True,
        task_execution_timeout=timedelta(hours=3),
        resources=TaskServicePodResources.custom(
            request_cpu="2",
            request_memory="20Gi",
            limit_memory="20Gi",
        ),
        alert_channel=team_slack_channel,
        dag_tsg='https://thetradedesk.atlassian.net/wiki/x/cICoI',
        enable_slack_alert=True,
        configuration_overrides={
            "ThirdPartyDataRatesCDCValidation.MaxRetries": f"{validation_task_max_retries}",
        }
    )
)
