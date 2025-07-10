from datetime import datetime

# Required import to add them to the list of dags
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import partportal

registry = TaskServiceDagRegistry(globals())

registry.register_dag(
    TaskServiceDagFactory(
        task_name="ApiTokenNotificationsTask",
        scrum_team=partportal,
        task_config_name="ApiTokenNotificationsConfig",
        start_date=datetime(2024, 8, 1),
        job_schedule_interval="0 0 * * 0",
        resources=TaskServicePodResources.medium()
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataSegmentsExportTask",
        scrum_team=partportal,
        task_config_name="ThirdPartyDataSegmentsExportTaskConfig",
        start_date=datetime(2024, 10, 21),
        job_schedule_interval="*/1 * * * *",
        resources=TaskServicePodResources.custom(
            request_cpu="1",
            request_memory="2Gi",
            limit_memory="4Gi",
            # The task downloads results from Algolia and stores in the local filesystem
            # The Algolia index contains ~13Gb of data total, so a limit of 16Gb should be more than enough
            request_ephemeral_storage="1Gi",
            limit_ephemeral_storage="16Gi"
        )
    )
)
