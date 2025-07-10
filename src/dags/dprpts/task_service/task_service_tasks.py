from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_connection_helper import azure
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from typing import Optional
import dags.dprpts.task_service.db_source as dbs


def create_sr_testing_task(
    registry: TaskServiceDagRegistry, branch_name: str, test_id: str, metadata_version: Optional[str], db_source=dbs.Production()
):
    # default db_source is "dbs.Production()"
    # Other db_source: "dbs.SandBox()" , "dbs.CloudSpin("IPADDRESS")"

    telnet_commands = ["changeField ReportProviderSourceCircuitBreaker.CircuitBreakerEnabledFeatureSwitch.Enabled false"]

    if metadata_version is not None:
        telnet_commands.append("changeField MyReportsGlobalMetadataFactory.UseYamlProducerFeature.Enabled true")
        telnet_commands.append(f"changeField MyReportsGlobalMetadataFactory.TargetMetadataVersion {metadata_version}")

    configuration = {
        "TestId": test_id,  # The TestId has to match the one you used to mark the Schedule
        "ScheduledReporting.Enabled": "True",  # enable ScheduledReporting
        "ScheduledReporting.AllowedReportProviderIds": "1,2,3,4,5",  # Allow all report providers
        "ScheduledReporting.AllowedReportProviderSourceIds": "3,4,8,9,13,101,103,17",  # Allow all report provider source
        "ScheduledReporting.RunOnce": "true"  # Execute the ScheduledReportingTask in RunOnce mode
    }

    if db_source.get_datasource() is not None:
        dbstring = f"Data Source={db_source.get_datasource()};Initial Catalog=Provisioning;MultipleActiveResultSets=True;Integrated Security=false;user id=ttd_task;password=pass!word1;MultiSubnetFailover=True;"
        configuration["ScheduledReporting.SourceDatabaseConnectionString"] = dbstring  # DBConnection which Schedules are read from.
    else:
        # production ; do nothing.
        pass

    return registry.register_dag(
        TaskServiceDagFactory(
            branch_name=branch_name,
            # This is the adplatform branch you want to test
            task_name="ScheduledReportingTask",
            task_config_name="ScheduledReportingTaskConfig",
            scrum_team=dprpts,
            start_date=datetime.now() - timedelta(hours=3),
            job_schedule_interval=None,
            alert_channel="#scrum-insights-alerts",
            resources=TaskServicePodResources.medium(),
            configuration_overrides=configuration,
            telnet_commands=telnet_commands
        )
    )


walmartSalesDataImportResource = TaskServicePodResources.custom(
    request_cpu="1", request_memory="2Gi", limit_memory="4Gi", limit_ephemeral_storage="10Gi"
)
registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="WalmartSalesDataAgileConsistencyCheckTask",
        task_config_name="WalmartSalesDataAgileConsistencyCheckConfigAgileWalmartSalesData30",
        task_name_suffix="AgileWalmartSalesData30",
        scrum_team=dprpts,
        start_date=datetime(2023, 1, 15),
        job_schedule_interval="0 * * * *",
        cloud_provider=azure,
        alert_channel="#scrum-insights-alerts",
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="WalmartSalesDataAgileConsistencyCheckTask",
        task_config_name="WalmartSalesDataAgileConsistencyCheckConfigAgileWalmartSalesData14",
        task_name_suffix="AgileWalmartSalesData14",
        scrum_team=dprpts,
        start_date=datetime(2023, 1, 15),
        job_schedule_interval="0 * * * *",
        cloud_provider=azure,
        alert_channel="#scrum-insights-alerts",
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="WalmartImportTask",
        task_config_name="WalmartImportTaskConfigForSalesData",
        task_name_suffix="WalmartSalesData",
        scrum_team=dprpts,
        start_date=datetime(2023, 4, 11),
        job_schedule_interval="0 * * * *",
        cloud_provider=azure,
        alert_channel="#scrum-dp-rpts-alerts",
        configuration_overrides={"WalmartSalesDataImport.UploadsContainer": "walmartsalesreports-v02"},
        resources=walmartSalesDataImportResource
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="WalmartImportTask",
        task_config_name="WalmartImportTaskConfigForNewBuyerReport",
        task_name_suffix="WalmartNewBuyerReport",
        scrum_team=dprpts,
        start_date=datetime(2023, 4, 11),
        job_schedule_interval="0 * * * *",
        cloud_provider=azure,
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaEonAutoScaleTask",
        task_config_name="VerticaEonAutoScaleTaskConfig",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTaskOfficialVerticaDriver",
        task_config_name="WalmartNewBuyerConsistencyCheckConfigVerticaWalmartNewBuyer",
        task_name_suffix="WalmartNewBuyerConsistencyCheck",
        scrum_team=dprpts,
        cloud_provider=azure,
        start_date=datetime(2023, 11, 1),
        job_schedule_interval="0 * * * *",
        alert_channel="#scrum-insights-alerts",
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FailedReportHandlingTask",
        task_config_name="ReportHandlingConfig",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/5 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
        retries=0,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="EmptyReportHandlingTask",
        task_config_name="ReportHandlingConfig",
        scrum_team=dprpts,
        start_date=datetime(2025, 4, 3),
        job_schedule_interval="30 12 * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
        retries=0,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SendEmailWhenReportDisabledTask",
        task_config_name="SendEmailWhenReportDisabledTaskConfig",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="@hourly",
        alert_channel="#scrum-insights-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SendEmailWhenDeprecatedPhysicalTableGroupTask",
        task_config_name="SendEmailWhenDeprecatedPhysicalTableGroupTaskConfig",
        scrum_team=dprpts,
        start_date=datetime(2024, 8, 13, 12, 0),
        job_schedule_interval="@daily",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTask",
        task_config_name="AcrBidFeedbackReportDailyCheckConfig",
        task_name_suffix="AcrBidFeedbackReportDailyCheck",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTask",
        task_config_name="AcrBidFeedbackReportDailyGNCheckConfig",
        task_name_suffix="AcrBidFeedbackReportDailyGNCheck",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTask",
        task_config_name="AcrDemosReportWeeklyCheckConfig",
        task_name_suffix="AcrDemosReportWeeklyCheck",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTask",
        task_config_name="AcrReportDailyCheckConfig",
        task_name_suffix="AcrReportDailyCheck",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTask",
        task_config_name="AcrReportDailyGNCheckConfig",
        task_name_suffix="AcrReportDailyGNCheck",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="PublishMyReportsMetadataTask",
        task_config_name="PublishMyReportsMetadataTaskConfig",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel="#scrum-dp-rpts-alerts",
        resources=TaskServicePodResources.small(),
    )
)
