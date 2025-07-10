from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.k8s_connection_helper import alicloud
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import MEASUREMENT, MEASUREMENT_UPPER, MEASURE_TASKFORCE_MEX
from ttd.task_service.persistent_storage.persistent_storage_config import PersistentStorageConfig, PersistentStorageType

disabledDBMetricsConfigOverrides = {
    "WriteMetricsToDatabase": "false",
}

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AtomMovieConversionImportTask",
        task_config_name="AtomMovieConversionImportTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ComscoreReportRetrievalTask",
        task_config_name="ComscoreReportRetrievalConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 9, 4, 10),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=3),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ComscoreReportSchedulerTask",
        task_config_name="ComscoreReportSchedulerConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 9, 4, 10),
        job_schedule_interval=timedelta(minutes=5),
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=3),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ComscoreCcrMetadataSyncTask",
        task_config_name="ComscoreCcrMetadataSyncTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2024, 4, 20, 0),
        job_schedule_interval=timedelta(days=1),
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=12),
        configuration_overrides=disabledDBMetricsConfigOverrides,
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="ConversionLiftGraphMismatchNotifierTask",
        task_config_name="ConversionLiftGraphMismatchNotifierTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime(2023, 10, 26),
        job_schedule_interval="0 0 */1 * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="EnableAndUpdateSkadnCampaignsTask",
        scrum_team=MEASUREMENT.team,
        task_config_name="EnableAndUpdateSkadnCampaignsTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="IntegralDataImportTask",
        task_config_name="IntegralDataImportTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2024, 4, 16),
        job_schedule_interval="0 2,10,18 */1 * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=18),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.ONE_POD, "1000Gi", mount_path="/tmp"),
        telnet_commands=[
            "invokeMethod IntegralDataImportTask.SkipDates.Add '2024-05-07'",
            "invokeMethod IntegralDataImportTask.SkipDates.Add '2024-05-13'",
        ],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FactualOfflineUniverseRefreshTask",
        task_config_name="FactualOfflineUniverseRefreshTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/12 * * * *",
        resources=TaskServicePodResources.large(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="IriOfflineUniverseRefreshTask",
        task_config_name="IriOfflineUniverseRefreshTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/12 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="LucidMetadataImportTask",
        task_config_name="LucidMetadataImportTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 9, 20),
        job_schedule_interval="0 1 */1 * *",
        run_only_latest=True,
        resources=TaskServicePodResources.medium(),
        retries=0,
        max_active_runs=1,
        task_execution_timeout=timedelta(hours=2),
        telnet_commands=[
            "try changeField LucidMetadataImportTask.Enabled false",
        ],
        configuration_overrides=disabledDBMetricsConfigOverrides,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="LucidStudyReportRetrievalTask",
        task_config_name="LucidStudyReportRetrievalTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 9, 20),
        job_schedule_interval="0 2,14 * * *",
        run_only_latest=True,
        resources=TaskServicePodResources.medium(),
        retries=0,
        max_active_runs=1,
        task_execution_timeout=timedelta(hours=8),
        telnet_commands=[
            "try changeField LucidStudyReportRetrievalTask.Enabled true",
        ],
        configuration_overrides=disabledDBMetricsConfigOverrides,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="LucidStudySchedulerTask",
        task_config_name="LucidStudySchedulerTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 9, 20),
        job_schedule_interval="0 */6 * * *",
        run_only_latest=True,
        resources=TaskServicePodResources.medium(),
        retries=0,
        max_active_runs=1,
        task_execution_timeout=timedelta(hours=4),
        telnet_commands=[
            "try changeField LucidStudySchedulerTask.Enabled true",
            "try changeField PoliticalTicketHelper.EnablePoliticalJiraIntegration.Enabled true"
        ],
        configuration_overrides=disabledDBMetricsConfigOverrides,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MeasurementSlackPosterTask",
        task_config_name="MeasurementSlackPosterTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=2),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NcsIfoTask",
        task_config_name="NcsIfoTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime(2023, 9, 19),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NcsPurchaseDataMetricsTask",
        task_config_name="NcsPurchaseDataMetricsTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=5),  # Runs every 5 mins, but sometimes take much longer than this
        resources=TaskServicePodResources.custom(
            request_memory="16Gi",
            request_cpu="1",
            limit_memory="32Gi",
            limit_ephemeral_storage="32Gi",
            request_ephemeral_storage="16Gi",
        ),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenDailyOTPReportingAggregateTask",
        task_config_name="NielsenDailyOTPReportingAggregateConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 8, 31),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenDailyOTPReportingIncrementCalculationTask",
        task_config_name="NielsenDailyOTPReportingIncrementCalculationConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 8, 31),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenDailyOTPReportingTask",
        task_config_name="NielsenDailyOTPReportingTaskConfig",
        configuration_overrides={
            "NielsenDailyOTPReporting.SkipProvisioning": "false",
            "NielsenDailyOTPReporting.ProcessNielsenDarFiles": "false",
            "NielsenDailyOTPReporting.ProcessNielsenOneFiles": "true",
            "NielsenDailyOTPReporting.CopyNielsenDarFilesToTestBucket": "false",
            "NielsenDailyOTPReporting.CopyNielsenOneFilesToTestBucket": "false",
            "NielsenDailyOTPReporting.UseNielsenOneLogFilesForLW": "true",
            "NielsenDailyOTPReporting.CopyLogFilesToLWBucket": "true",
            "NielsenDailyOTPReporting.AllowNotifyLWDB": "true",
        },
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 11, 6),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.custom(request_cpu="1", request_memory="2Gi", limit_memory="4Gi", limit_ephemeral_storage="10Gi"),
        run_only_latest=True
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenOneBrandPullTask",
        task_config_name="NielsenOneBrandPullTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2024, 6, 27),
        job_schedule_interval="0 1 */1 * *",
        resources=TaskServicePodResources.medium(),
        run_only_latest=True
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NielsenDataImportBySftpTask",
        task_config_name="NielsenDataImportBySftpTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 8, 31),
        job_schedule_interval="0 */4 * * *",
        resources=TaskServicePodResources.custom(request_cpu="1", request_memory="8Gi", limit_memory="12Gi", limit_ephemeral_storage="5Gi"),
        configuration_overrides={
            "NielsenDataImportBySftp.UseNielsenOneForUpdateFileStatuses": "true",
        }
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ContextualInferredBrandIntentS3ConversionsImportTask",
        task_config_name="ContextualInferredBrandIntentS3ConversionsImportTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime(2023, 9, 6),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.custom(request_cpu="1", request_memory="4Gi", limit_memory="8Gi", limit_ephemeral_storage="1Gi"),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="InferredBrandImpactS3ConversionsImportTask",
        task_config_name="InferredBrandImpactS3ConversionsImportTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime(2023, 10, 4),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.large(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="NewDevicesIndicatorImportTask",
        task_config_name="NewDevicesIndicatorImportTaskConfig",
        scrum_team=MEASUREMENT.team,
        start_date=datetime(2023, 10, 12),
        job_schedule_interval="0 8 * * 3",
        configuration_overrides={
            "NewDevicesIndicator.BaseS3BucketUrl": "ttd-insights",
        },
        resources=TaskServicePodResources.custom(
            request_memory="16Gi",
            request_cpu="1",
            limit_memory="32Gi",
            limit_ephemeral_storage="32Gi",
            request_ephemeral_storage="16Gi",
        ),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="CarrierActivationsIndicatorImportTask",
        task_config_name="CarrierActivationsIndicatorImportTaskConfig",
        scrum_team=MEASURE_TASKFORCE_MEX.team,
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
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MiaozhenCmsCnCampaignCreationTask",
        task_config_name="MiaozhenOtpTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 11, 10),
        job_schedule_interval=timedelta(minutes=5),
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
        run_only_latest=True
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MiaozhenCmsCnReportDownloadTask",
        task_config_name="MiaozhenOtpReportDownloadTaskConfig",
        scrum_team=MEASUREMENT_UPPER.team,
        start_date=datetime(2023, 11, 10),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.custom(request_cpu="1", request_memory="2Gi", limit_memory="4Gi", limit_ephemeral_storage="10Gi"),
        cloud_provider=alicloud,
        run_only_latest=True
    )
)
