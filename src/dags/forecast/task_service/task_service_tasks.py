from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import FORECAST

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AnaplanImportTask",
        task_config_name="AnaplanImportTaskConfig",
        scrum_team=FORECAST.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="CampaignForecastValidationTask",
        task_config_name="CampaignForecastValidationTaskConfig",
        scrum_team=FORECAST.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="RamForecastingModelEvaluationStableAdGroupsTask",
        task_config_name="RamForecastingModelEvaluationStableAdGroupsTaskConfig",
        scrum_team=FORECAST.team,
        start_date=datetime(2024, 12, 23),
        job_schedule_interval="0 */3 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ProvisioningCleanupRamTask",
        task_config_name="ProvisioningCleanupRamTaskConfig",
        scrum_team=FORECAST.team,
        start_date=datetime(2024, 8, 27),
        job_schedule_interval="0 */24 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="RecalculateRamTask",
        task_config_name="RecalculateRamTaskConfig",
        scrum_team=FORECAST.team,
        start_date=datetime(2024, 8, 27),
        job_schedule_interval="0 */5 * * *",
        resources=TaskServicePodResources.custom(request_cpu="4", request_memory="32Gi", limit_memory="48Gi"),
        task_execution_timeout=timedelta(hours=7),
        telnet_commands=[
            "try changeField RamCalcAdGroupRequestMapper.AdditionalLogging false",
            "try changeField RecalculateRamTask.EnableDebugging false",
        ],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="OpenMarketWinRateUpdateTask",
        task_config_name="OpenMarketWinRateUpdateTaskConfigUsEast01",
        task_name_suffix="_USEast01",
        scrum_team=FORECAST.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=9),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="OpenMarketWinRateUpdateTask",
        task_config_name="OpenMarketWinRateUpdateTaskConfigUsWest01",
        task_name_suffix="_USWest01",
        scrum_team=FORECAST.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=9),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ForecastRunnerTask",
        task_config_name="ForecastRunnerTaskConfig",
        scrum_team=FORECAST.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.custom(request_cpu="2", request_memory="8Gi", limit_memory="16Gi"),
    )
)
