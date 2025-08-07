from datetime import datetime
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import MEW

registry = TaskServiceDagRegistry(globals())

registry.register_dag(
    TaskServiceDagFactory(
        task_name="CarrierActivationsIndicatorImportTask",
        task_config_name="CarrierActivationsIndicatorImportTaskConfig",
        scrum_team=MEW.team,
        start_date=datetime(2024, 9, 12),
        job_schedule_interval="0 8 * * 3",
        resources=TaskServicePodResources.custom(
            request_memory="16Gi",
            request_cpu="1",
            limit_memory="32Gi",
            limit_ephemeral_storage="32Gi",
            request_ephemeral_storage="16Gi",
        ),
    )
)
