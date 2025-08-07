from datetime import datetime
# from airflow import DAG

from ttd.slack import slack_groups
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.task_service_dag import TaskServiceDagRegistry, TaskServiceDagFactory
from ttd.ttdenv import TtdEnvFactory

env = TtdEnvFactory.get_from_system()
is_prod = env == TtdEnvFactory.prod

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    # Run daily at approximately after Contextual Seeds job completes the main batch
    TaskServiceDagFactory(
        task_name="ContextualRsmSeedRefreshTask",
        scrum_team=slack_groups.CTXMP.team,
        task_config_name="ContextualRsmSeedRefreshTaskConfig",
        start_date=datetime(2025, 7, 15),
        job_schedule_interval="0 3 * * *",
        retries=1 if is_prod else 0,
        resources=TaskServicePodResources.small()
    )
)
