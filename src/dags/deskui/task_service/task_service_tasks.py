from datetime import datetime
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import deskui

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GenerateMcuCheatsheetsTask",
        task_config_name="GenerateMcuCheatsheetsTaskConfig",
        scrum_team=deskui,
        start_date=datetime(2024, 5, 1),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.custom(
            request_cpu="2",
            request_memory="8Gi",
            limit_memory="16Gi",
            limit_ephemeral_storage="1Gi",
        )
    )
)
