from datetime import datetime
# from airflow import DAG
from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.task_service_dag import TaskServiceDagRegistry, TaskServiceDagFactory

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AutoOptimizationTask",
        task_config_name="AutoOptimizationTaskConfig",
        scrum_team=dprpts,
        start_date=datetime(2025, 4, 5, 12, 0),
        job_schedule_interval="0 14 * * *",
        max_active_runs=1,
        alert_channel="#dev-agiles-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
