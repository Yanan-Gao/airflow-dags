from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import DATASRVC
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AvailsStreamDigestTask",
        task_config_name="AvailsStreamDigestTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(hours=1),
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AvailsStreamLagCheckTask",
        task_config_name="AvailsStreamLagCheckTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(minutes=15),
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="IdMappingTask",
        task_config_name="IdMappingTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(hours=1),
        resources=TaskServicePodResources.large(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="RedsUsageS3ToSqlTask",
        task_config_name="RedsUsageS3ToSqlTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(hours=4),
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=4)
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SnowflakeProvisioningChangeTrackingTask",
        task_config_name="SnowflakeProvisioningChangeTrackingTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(hours=12),
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=2),
        telnet_commands=[
            "try changeField SnowflakeProvisioningChangeTrackingTask.ForceNextSync false",
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SnowflakeVerticaConsistencyCheckTask",
        task_config_name="SnowflakeConsistencyCheckConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(days=1),
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SnowflakeDataBackFillTask",
        task_config_name="SnowflakeDataBackFillTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval=timedelta(days=1),
        task_execution_timeout=timedelta(hours=4),
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaAdhocCleanupTask",
        task_config_name="VerticaAdhocCleanupTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2025, 6, 1),
        job_schedule_interval="0 6 * * *",
        telnet_commands=["change VerticaAdhocCleanupTask.TablesFilter \"and not is_temp_table\""],
        resources=TaskServicePodResources.medium()
    )
)

snowflake_data_scrubbing_task_config_names = [
    "TheTradeDeskSnowflakeDataScrubbingTaskConfig", "RedsAwsUsEast1SnowflakeDataScrubbingTaskConfig",
    "RedsAwsApSoutheast2SnowflakeDataScrubbingTaskConfig", "RedsAwsEuWest1SnowflakeDataScrubbingTaskConfig",
    "RedsAzureEastUs2SnowflakeDataScrubbingTaskConfig", "RedsAzureWestUs2SnowflakeDataScrubbingTaskConfig"
]

for config_name in snowflake_data_scrubbing_task_config_names:
    registry.register_dag(
        TaskServiceDagFactory(
            task_name="SnowflakeDataScrubbingTask",
            task_name_suffix=config_name.replace("SnowflakeDataScrubbingTaskConfig", ""),
            task_config_name=config_name,
            scrum_team=DATASRVC.team,
            start_date=datetime(2025, 6, 1),
            job_schedule_interval=timedelta(days=1),
            resources=TaskServicePodResources.medium(),
            telnet_commands=[
                "try changeField SnowflakeDataScrubbingTaskConfig.SnowflakeCommandTimeout 1800",
            ]
        )
    )

vertica_data_purge_task_config_names = [
    # "UsEast01VerticaDataPurgeTaskConfig", exclude this since we need longer runtime for etl cluster
    "UsEast02UiVerticaDataPurgeTaskConfig",
    "UsEast03VerticaDataPurgeTaskConfig",
    # "UsWest01VerticaDataPurgeTaskConfig", exclude this since we need longer runtime for etl cluster
    "UsWest02UiVerticaDataPurgeTaskConfig",
    "UsWest03VerticaDataPurgeTaskConfig",
    "UsEast01ForecastingVerticaDataPurgeTaskConfig",
    "CNEast01VerticaDataPurgeTaskConfig",
    "CNWest01VerticaDataPurgeTaskConfig"
]

for config_name in vertica_data_purge_task_config_names:
    registry.register_dag(
        TaskServiceDagFactory(
            task_name="VerticaDataPurgeTask",
            task_name_suffix=config_name.replace("VerticaDataPurgeTaskConfig", ""),
            task_config_name=config_name,
            scrum_team=DATASRVC.team,
            start_date=datetime(2025, 6, 1),
            task_execution_timeout=timedelta(hours=4),
            retries=1,
            job_schedule_interval=timedelta(hours=4),
            resources=TaskServicePodResources.medium(),
        )
    )

registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaDataPurgeTask",
        task_name_suffix="UsEast01",
        task_config_name="UsEast01VerticaDataPurgeTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(year=2025, month=2, day=1),
        task_execution_timeout=timedelta(hours=12),
        retries=1,
        job_schedule_interval="0 6 * * *",  # Run at UTC 06:00
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaDataPurgeTask",
        task_name_suffix="UsWest01",
        task_config_name="UsWest01VerticaDataPurgeTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(year=2025, month=2, day=1),
        task_execution_timeout=timedelta(hours=12),
        retries=1,
        job_schedule_interval="0 6 * * *",  # Run at UTC 06:00
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="ExposureFeedAutoCompleteTask",
        task_config_name="ExposureFeedAutoCompleteTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(year=2024, month=12, day=18),
        job_schedule_interval=timedelta(hours=1),
        resources=TaskServicePodResources.medium()
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="WalmartRedsFeedTask",
        task_config_name="WalmartRedsFeedTaskConfig",
        scrum_team=DATASRVC.team,
        start_date=datetime(2024, 10, 1),
        job_schedule_interval=timedelta(days=1),
        resources=TaskServicePodResources.small(),
        configuration_overrides={
            "WalmartRedsFeedTask.DryRun": "false",
            "WalmartRedsFeedTask.TestPartner": ""
        },
    )
)
