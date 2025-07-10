from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import CTV

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenAmrldParquetUploadTask",
        task_config_name="NielsenAmrldParquetUploadTaskConfig",
        task_name_suffix="NielsenAmrldParquetUploadTaskDaily",
        scrum_team=CTV.team,
        start_date=datetime(2023, 9, 18),
        job_schedule_interval="0 9 * * *",
        resources=TaskServicePodResources
        .custom(request_cpu="2", request_memory="8Gi", request_ephemeral_storage="2Gi", limit_memory="32Gi", limit_ephemeral_storage="4Gi"),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenNdsLoaderTask",
        task_config_name="NielsenNdsLoaderTaskConfig",
        scrum_team=CTV.team,
        start_date=datetime(2024, 12, 23),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenOttAdGroupReportingTask",
        task_config_name="NielsenOttReportingTaskConfig",
        scrum_team=CTV.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenOttCampaignReportingTask",
        task_config_name="NielsenOttReportingTaskConfig",
        scrum_team=CTV.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenAdIntelParquetUploadTaskV2",
        task_config_name="NielsenAdIntelParquetUploadTaskV2Config",
        task_name_suffix="NielsenAdIntelParquetUploadTaskV2Daily",
        scrum_team=CTV.team,
        start_date=datetime(2023, 9, 18),
        job_schedule_interval="0 9 * * *",
        resources=TaskServicePodResources.large(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TvVerticaConsistencyCheckTask",
        task_config_name="CampaignReachAndFrequencyDailyCheckConfig",
        task_name_suffix="CampaignReachAndFrequencyDailyCheck",
        scrum_team=CTV.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TvVerticaConsistencyCheckTask",
        task_config_name="FrequencyReportConfig",
        task_name_suffix="FrequencyReport",
        scrum_team=CTV.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TvVerticaConsistencyCheckTask",
        task_config_name="FrequencySavingStatsConfig",
        task_name_suffix="FrequencySavingStats",
        scrum_team=CTV.team,
        start_date=datetime(2024, 12, 23),
        job_schedule_interval="0 */3 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
