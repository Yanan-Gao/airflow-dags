# from airflow import DAG
from datetime import datetime, timedelta
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import sav

config_overrides = {
    "PharmaNotifierTask.SlackWebhookUrl": "{{ var.value.pharma_alerts_slack_webhook }}",
    "PharmaNotifierTask.StartTime": "{{ data_interval_start.subtract(minutes=5).to_datetime_string() }}",
    "PharmaNotifierTask.EndTime": "{{ data_interval_end.subtract(minutes=5).to_datetime_string() }}",
}

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="PharmaNotifierTask",
        task_config_name="PharmaNotifierTaskConfig",
        scrum_team=sav,
        start_date=datetime(2025, 2, 7),
        task_execution_timeout=timedelta(minutes=5),
        job_schedule_interval="0 */4 * * *",  # Run every four hours
        configuration_overrides=config_overrides,
    )
)
