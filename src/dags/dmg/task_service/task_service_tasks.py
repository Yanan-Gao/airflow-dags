from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import DEAL_MANAGEMENT

registry = TaskServiceDagRegistry(globals())

registry.register_dag(
    TaskServiceDagFactory(
        task_name="DealMetadataProvisioningSyncTask",
        scrum_team=DEAL_MANAGEMENT.team,
        task_config_name="DealMetadataProvisioningTaskSyncConfig",
        start_date=datetime.now() - timedelta(hours=3),
        task_execution_timeout=timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        retries=0,
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Deal+Metadata+Stack",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DealMetadataS3ToAvailsTask",
        scrum_team=DEAL_MANAGEMENT.team,
        task_config_name="DealMetadataS3ToAvailsTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=10),  # Scheduled for every 10 mins but can take 2+ hr
        task_execution_timeout=timedelta(hours=24),
        retries=0,
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Deal+Metadata+Stack",
        telnet_commands=["try changeField DealMetadataS3ToAvailsTask.ImplementInventoryChannelsGrain.Enabled true"]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ProposalServingStatusSyncTask",
        scrum_team=DEAL_MANAGEMENT.team,
        task_config_name="ProposalServingStatusSyncTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Proposal+Serving+Status+Sync+Task+Error"
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdXClientAccountSyncTask",
        scrum_team=DEAL_MANAGEMENT.team,
        task_config_name="AdXClientAccountSyncTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=5),
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/AdX+Account+Sync+Error"
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="InventoryKokaiBackFillMigrationTask",
        scrum_team=DEAL_MANAGEMENT.team,
        task_config_name="InventoryKokaiBackFillMigrationTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        task_execution_timeout=timedelta(hours=3),
        retries=0,
        resources=TaskServicePodResources.medium()
    )
)
