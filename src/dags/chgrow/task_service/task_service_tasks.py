from datetime import datetime
# Required comment to add them to the list of dags
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import CHGROW

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="PublicaAdvertiserPreferenceSyncTask",
        task_config_name="PublicaAdvertiserPreferenceSyncConfig",
        scrum_team=CHGROW.channels_growth(),
        start_date=datetime(2023, 4, 4),
        job_schedule_interval="0 * * * *",
        slack_tags=CHGROW.channels_growth().sub_team
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="BudgetAvailsScheduleGenerationTask",
        task_config_name="BudgetAvailsScheduleGenerationTaskConfig",
        scrum_team=CHGROW.channels_growth(),
        start_date=datetime(2025, 6, 16),
        job_schedule_interval="0 * * * *",
        slack_tags=CHGROW.channels_growth().sub_team,
        service_name="contentlibrary",
        telnet_commands=[
            "invoke BudgetAvailsScheduleGenerationTask.PublishEnabled.SetEnabled",
            "invoke BudgetAvailsScheduleGenerationTask.ThrowExceptionWhenGetLiveEventFails.SetEnabled"
        ]
    )
)
