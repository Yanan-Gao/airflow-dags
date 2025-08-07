from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import IDENTITY

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdBrainHealthTask",
        scrum_team=IDENTITY.team,
        task_config_name="AdBrainHealthConfig",
        start_date=datetime(2024, 7, 30),
        # Graphable avails run on Sunday. This gives padding in case there is a delay in generating the report.
        job_schedule_interval="0 12 * * MON",
        resources=TaskServicePodResources.custom(request_cpu="2", request_memory="4Gi", limit_memory="8Gi", limit_ephemeral_storage="4Gi"),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="LiveRampOptOutImportTask",
        scrum_team=IDENTITY.team,
        task_config_name="LiveRampOptOutImportTaskConfig",
        start_date=datetime(2024, 9, 26),
        job_schedule_interval="0 */4 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="UniversalCrossDeviceCollectorTask",
        scrum_team=IDENTITY.team,
        task_config_name="UniversalCrossDeviceCollectorTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            # Upload the test graph (CrossDeviceVendorID 14) to VAE HotCache only (TaskVariantID 121, DataServerVae).
            "invokeMethod UniversalCrossDeviceCollectorTask.AddDataServerGraphLoadToTaskVariantOverride 14 121",
            # Ignore hot cache load for iav2 legacy vendors
            "invokeMethod UniversalCrossDeviceCollectorTask.AddDataServerGraphLoadToTaskVariantOverride 610 -1",
            "invokeMethod UniversalCrossDeviceCollectorTask.AddDataServerGraphLoadToTaskVariantOverride 611 -1",
            "invokeMethod UniversalCrossDeviceCollectorTask.GetDataServerGraphLoadOverrides"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="LiveRampCrosswalkImporterTask",
        scrum_team=IDENTITY.team,
        task_config_name="LiveRampCrosswalkImporterTaskConfig",
        start_date=datetime(2024, 9, 25),
        job_schedule_interval="0 */12 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="IdntVerticaConsistencyCheckTask",
        task_config_name="IAv2DataElementReportDailyCheckConfig",
        scrum_team=IDENTITY.team,
        start_date=datetime(2024, 12, 23),
        job_schedule_interval="0 */3 * * *",
        telnet_commands=[
            "try changeField IAv2DataElementReportDailyCheckConfig.MarginWindowInHoursToPerformCheck 72.0",
            "try changeField VerticaConsistencyCheckTaskTelnets.SkipChinaVerticaCluster true"
        ],
        resources=TaskServicePodResources.medium()
    )
)
