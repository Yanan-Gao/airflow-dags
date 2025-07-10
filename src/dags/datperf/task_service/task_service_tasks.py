from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import DATPERF

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AutoOptModelCalculationEngineTask",
        task_config_name="AutoOptModelCalculationEngineTaskConfig",
        scrum_team=DATPERF.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=15),  # Scheduled for every 15 mins, but can take 25+ mins
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=8)
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ApplyAdGroupActivityLogBatchTask",
        task_config_name="ApplyAdGroupActivityLogBatchTaskConfig",
        scrum_team=DATPERF.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ApplyAdGroupOptimizationBatchTask",
        task_config_name="ApplyAdGroupOptimizationBatchTaskConfig",
        scrum_team=DATPERF.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=30),  # Scheduled for every 30 mins, but can take 30-35 mins
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="S3ToSqlDbDataImportPartitionProcessingTask",
        task_config_name="S3ToSqlDbDataImportTaskConfig",
        scrum_team=DATPERF.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(hours=1),  # Scheduled for every hr, but can take 2+ hours
        max_active_runs=1,
        run_only_latest=True,
        resources=TaskServicePodResources.
        custom(request_cpu="4", request_memory="8Gi", limit_memory="16Gi", request_ephemeral_storage="8Gi", limit_ephemeral_storage="16Gi"),
        task_execution_timeout=timedelta(hours=8),
        telnet_commands=["changeField AdGroupImpressionProfileDataImport.PurgeSprocTimeout 7200"]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="S3ToSqlDbDataImportPartitionDiscoveryTask",
        task_config_name="S3ToSqlDbDataImportTaskConfig",
        scrum_team=DATPERF.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=30),
        task_execution_timeout=timedelta(minutes=30)
    )
)
