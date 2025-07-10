from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.k8s_connection_helper import alicloud
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import dataproc, AIFUN

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DatalakeConsistencyCheckTask",
        scrum_team=dataproc,
        task_config_name="DailyDatalakeConsistencyCheckConfig",
        task_name_suffix="daily",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[AIFUN.team.jira_team],
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DatalakeConsistencyCheckTask",
        scrum_team=dataproc,
        task_config_name="DailyDatalakeConsistencyCheckConfig",
        task_name_suffix="daily-china",
        cloud_provider=alicloud,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[AIFUN.team.jira_team],
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DatalakeConsistencyCheckTask",
        scrum_team=dataproc,
        task_config_name="HourlyDatalakeConsistencyCheckConfig",
        task_name_suffix="hourly",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[AIFUN.team.jira_team],
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DatalakeConsistencyCheckTask",
        scrum_team=dataproc,
        task_config_name="HourlyDatalakeConsistencyCheckConfig",
        task_name_suffix="hourly-china",
        cloud_provider=alicloud,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[AIFUN.team.jira_team],
        enable_slack_alert=False,
    )
)
