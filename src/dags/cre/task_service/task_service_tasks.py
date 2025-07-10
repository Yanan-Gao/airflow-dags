from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.k8s_connection_helper import alicloud
from ttd.slack.slack_groups import cre

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AppNexusCreativeAuditTask",
        task_config_name="AppNexusCreativeAuditTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AppNexusCreativeCheckTask",
        task_config_name="AppNexusCreativeCheckTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ClinchDataImportTask",
        task_config_name="ClinchDataImportTaskConfig",
        scrum_team=cre,
        start_date=datetime(2023, 8, 13),
        job_schedule_interval="0 3 */1 * *",
        task_execution_timeout=timedelta(hours=2),
        resources=TaskServicePodResources.
        custom(request_cpu="2", request_memory="4Gi", request_ephemeral_storage="15Gi", limit_memory="8Gi", limit_ephemeral_storage="60Gi")
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="CreativeInternalReviewTask",
        task_config_name="CreativeInternalReviewTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoogleCreativeApprovalCheckSubmittedCreativesTask",
        task_config_name="GoogleCreativeApprovalConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/60 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField GoogleCreativeSyncAllTask.LookbackWindowFilter.Enabled true",
            "try changeField GoogleCreativeSubmissionTask.DisableGoogleSupplyVendorCreativeApprovalStatusUpdate.Enabled True",
            "try changeField CreativeApprovalHelper.WriteDesLogForUnKnownAuditStatus true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoogleCreativeSubmissionTask",
        task_config_name="GoogleCreativeSubmissionConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        retries=4,  # default is 2
        retry_delay=timedelta(minutes=2),  # default is 5
        task_execution_timeout=timedelta(hours=2),
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField GoogleCreativeSyncAllTask.LookbackWindowFilter.Enabled true",
            "try changeField GoogleCreativeSubmissionTask.DisableGoogleSupplyVendorCreativeApprovalStatusUpdate.Enabled True",
            "try changeField CreativeApprovalHelper.WriteDesLogForUnKnownAuditStatus true",
            "try changeField AdTagHelper.RemoveSensitiveInfoWalmartTenant.Rate 1",
            "try changeField GoogleCreativeSubmissionTask.UseReadonlyProvDb.Enabled true",
            "try changeField GoogleCreativeSubmissionTask.MaxReturnCountForCreativesThatNeedToBeSubmitted 1000",
            "try changeField GoogleCreativeSubmissionTask.MaxDegreeOfParallelSubmissions 3",
            "try changeField GoogleCreativeSubmissionTask.NewCreativeSubmissionMinimumAllocation  200"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoogleCreativeSyncAllTask",
        task_config_name="GoogleCreativeSyncAllConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField GoogleCreativeSyncAllTask.LookbackWindowFilter.Enabled true",
            "try changeField GoogleCreativeSubmissionTask.DisableGoogleSupplyVendorCreativeApprovalStatusUpdate.Enabled True",
            "try changeField CreativeApprovalHelper.WriteDesLogForUnKnownAuditStatus true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VioohCreativeSubmissionTaskV2",
        task_config_name="VioohCreativeSubmissionTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField VioohCreativeSubmissionTaskV2.SinglePublisherIdUsedForAllCreatives JCDECAUX_GB",
            'invoke VioohCreativeSubmissionTaskV2.AddToConfigForClientConnectionOverrides "{""JCDECAUX_CN_SH_METRO"":{""VioohApiBaseUrl"":""https://moderation-api.prod.viooh.com.cn/api/v1/moderation/submission/creative/"",""VioohXSmartExchangeKey"":""ece9e30fdd10c6ebea741e0171c64110"",""AccountId"":2}}"',
            'invoke VioohCreativeSubmissionTaskV2.AddToConfigForClientConnectionOverrides "{""BJ_METRO_CN"":{""VioohApiBaseUrl"":""https://moderation-api.prod.viooh.com.cn/api/v1/moderation/submission/creative/"",""VioohXSmartExchangeKey"":""ece9e30fdd10c6ebea741e0171c64110"",""AccountId"":2}}"',
            'invoke VioohCreativeSubmissionTaskV2.AddToConfigForClientConnectionOverrides "{""JCDECAUX_CN_AIRPORT_PVG"":{""VioohApiBaseUrl"":""https://moderation-api.prod.viooh.com.cn/api/v1/moderation/submission/creative/"",""VioohXSmartExchangeKey"":""ece9e30fdd10c6ebea741e0171c64110"",""AccountId"":2}}"'
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustAdTagCleanupTask",
        task_config_name="TheMediaTrustAdTagCleanupTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField TheMediaTrustCreativeSyncTask.ExecuteLandingPageDetectionPolicy.Enabled true",
            "try changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPages.Enabled true",
            "try changeField TheMediaTrustAdTagCleanupTask.UseReadonlyProvDb.Enabled true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustAlertCheckTask",
        task_config_name="TheMediaTrustAlertCheckTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPages.Enabled false",
            "changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPagesRollout.Enabled false",
            "changeField TheMediaTrustAlertCheckTask.ExcludeLpViolationInGeneralPolicyViolation.Enabled true",
            "changeField TheMediaTrustAlertCheckTask.ExecuteWalmartPolicyViolationsReport.Enabled false",
        ]
    )
),
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustAlertCheckTask",
        task_config_name="TheMediaTrustAlertCheckTaskConfig",
        scrum_team=cre,
        start_date=datetime(2025, 1, 1),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        task_name_suffix="LandingPagePolicyViolation",
        dag_tsg="https://thetradedesk.atlassian.net/l/cp/c9K1jYhM",
        telnet_commands=[
            "changeField TheMediaTrustAlertCheckTask.ExecuteIncidentSummary false",
            "changeField TheMediaTrustAlertCheckTask.ExecutePolicyViolation false",
            "changeField TheMediaTrustAlertCheckTask.ExecuteWalmartPolicyViolationsReport.Enabled false",
            "changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPages.Enabled true",
            "changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPagesRollout.Enabled true",
            "changeField TheMediaTrustAlertCheckTask.ExcludeLpViolationInGeneralPolicyViolation.Enabled true",
            "changeField TheMediaTrustAlertCheckTask.EnableImportLpPolicyViolationInBatch.Enabled true",
        ]
    )
),
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustAlertCheckTask",
        task_config_name="TheMediaTrustAlertCheckTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="0 */4 * * *",
        resources=TaskServicePodResources.medium(),
        task_name_suffix="WalmartPolicyViolationTrueUp",
        telnet_commands=[
            "changeField TheMediaTrustAlertCheckTask.ExecuteIncidentSummary false",
            "changeField TheMediaTrustAlertCheckTask.ExecutePolicyViolation false",
            "changeField TheMediaTrustAlertCheckTask.ExecuteWalmartPolicyViolationsReport.Enabled true",
            "changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPages.Enabled false",
            "changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPagesRollout.Enabled false",
            "changeField TheMediaTrustAlertCheckTask.ExcludeLpViolationInGeneralPolicyViolation.Enabled true",
            "changeField TheMediaTrustAlertCheckTask.PolicyViolationRunBatchSizeInMinutes 360",
            "changeField TheMediaTrustAlertCheckTask.TmtApiClientSleepInSeconds 15",
            "changeField TheMediaTrustAlertCheckTask.PolicyViolationCheckDelayMinutes 60",
            "changeField TheMediaTrustAlertCheckTask.RunWalmartPolicyViolationsReportAsTrueup.Enabled true",
        ]
    )
),
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustCreativeSyncTask",
        task_config_name="TheMediaTrustCreativeSyncTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/3 * * * *",
        resources=TaskServicePodResources.medium(),
        retry_delay=timedelta(minutes=2),
        configuration_overrides={"TMTCreativeSync.BatchReadTimeout": "00:02:00"},
        telnet_commands=[
            "try changeField TheMediaTrustCreativeSyncTask.ExecuteAdvertiserLandingPageRolloutDetectionPolicy.Enabled true",
            "try changeField TheMediaTrustCreativeSyncTask.ExecuteLandingPageDetectionPolicy.Enabled true",
            "try changeField TheMediaTrustAlertCheckTask.FetchDetectedLandingPages.Enabled true",
            "try changeField AdTagHelper.RemoveSensitiveInfoWalmartTenant.Rate 1",
            "try changeField TheMediaTrustCreativeSyncTask.UseReadonlyProvDb.Enabled true",
            "try changeField TheMediaTrustCreativeSyncTask.ShouldThrowExceptionIfFailedProcessLPDetectionResult.Enabled true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FreewheelCreativeDealAssociationTask",
        task_config_name="FreewheelCreativeDealAssociationTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField FreewheelCreativeDealTask.PerPublisherVastVersioning.Enabled true",
            "try changeField FreewheelCreativeDealAssociationTask.UseReadonlyProvDb.Enabled true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FreewheelCreativeUpdateTask",
        task_config_name="FreewheelCreativeUpdateTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField FreewheelCreativeDealTask.PerPublisherVastVersioning.Enabled true",
            "try changeField FreewheelCreativeUpdateTask.UseReadonlyProvDb.Enabled true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FreeWheelCreativePreIngestionSyncTask",
        task_config_name="FreeWheelCreativePreIngestionSyncTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.small()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FreewheelCreativeDealAssociationStatusSyncTask",
        task_config_name="FreewheelCreativeDealAssociationStatusSyncTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/20 * * * *",
        resources=TaskServicePodResources.small()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="JivoxCreativeImportTask",
        task_config_name="JivoxCreativeImportTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="JivoxDataImportTask",
        task_config_name="JivoxDataImportTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="0 */1 * * *",
        task_execution_timeout=timedelta(hours=24),
        resources=TaskServicePodResources.custom(
            request_cpu="2", request_memory="64Gi", request_ephemeral_storage="15Gi", limit_memory="128Gi", limit_ephemeral_storage="60Gi"
        )
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="JivoxMetadataImportTask",
        task_config_name="JivoxMetadataImportTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
# ! Not including below because it runs every 1 min
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyNativeCreativeScanTask",
        task_config_name="ThirdPartyNativeCreativeScanConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/1 * * * *",
        telnet_commands=
        ["changeField VastDaastFeatureSwitches.EnableVast4 true", "changeField AdTagHelper.RemoveSensitiveInfoWalmartTenant.Rate 1"],
        resources=TaskServicePodResources.
        custom(request_cpu="4", request_memory="32Gi", request_ephemeral_storage="5Gi", limit_memory="64Gi", limit_ephemeral_storage="8Gi")
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TripleLiftAuditTask",
        task_config_name="TripleLiftAuditTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField TripleLiftApi.DynamicDescriptionCharLimit 200",
            "try changeField TripleLiftAuditTask.UseReadonlyProvDb.Enabled true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TripleLiftSubmitTask",
        task_config_name="TripleLiftSubmitTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField TripleLiftApi.DynamicDescriptionCharLimit 200",
            "try changeField TripleLiftSubmitTask.UseReadonlyProvDb.Enabled true"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ChinaSSPNotificationTask",
        task_config_name="ChinaSSPNotificationTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 24),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.medium(),
        configuration_overrides={"ChinaSSPNotificationTask.LookbackWindowInHours": "264"},
        telnet_commands=["changeField ChinaSSPNotificationTask.UseReadonlyProvDb.Enabled true"]
    )
)
# ! Not including below because it runs every 3 mins
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyAudioCreativeScanTask",
        task_config_name="ThirdPartyAudioVideoCreativeScanConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/3 * * * *",
        resources=TaskServicePodResources.
        custom(request_cpu="4", request_memory="32Gi", request_ephemeral_storage="5Gi", limit_memory="64Gi", limit_ephemeral_storage="8Gi"),
        telnet_commands=[
            "changeField VastDaastFeatureSwitches.EnableVast4 true",
            "change ThirdPartyAudioCreativeScanTask.EnableDoubleVerifyHeaders true",
            "changeField AdTagHelper.RemoveSensitiveInfoWalmartTenant.Rate 1"
        ]
    )
)
# ! Not including below because it runs every 3 mins
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyVideoCreativeScanTask",
        task_config_name="ThirdPartyAudioVideoCreativeScanConfig",
        scrum_team=cre,
        start_date=datetime(2024, 6, 9),
        job_schedule_interval="*/3 * * * *",
        resources=TaskServicePodResources.
        custom(request_cpu="4", request_memory="32Gi", request_ephemeral_storage="5Gi", limit_memory="64Gi", limit_ephemeral_storage="8Gi"),
        telnet_commands=[
            "changeField VastDaastFeatureSwitches.EnableVast4 true",
            "change ThirdPartyAudioVideoCreativeScanHelper.LocalTelemetry.LastRunDebugInfo.Enabled true",
            "changeField AdTagHelper.RemoveSensitiveInfoWalmartTenant.Rate 1"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyVideoCreativeMediaFileScanTask",
        task_config_name="ThirdPartyVideoCreativeMediaFileScanTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 12, 25),
        job_schedule_interval="*/3 * * * *",
        resources=TaskServicePodResources.
        custom(request_cpu="4", request_memory="32Gi", request_ephemeral_storage="5Gi", limit_memory="64Gi", limit_ephemeral_storage="8Gi"),
        telnet_commands=[
            "changeField ThirdPartyVideoCreativeMediaFileScanTask.BatchSize 2000",
            "invokeMethod ThirdPartyVideoCreativeMediaFileScanTask.SetMaxConcurrentDownloadsForHost gcdn.2mdn.net 10"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustLpDetectionReportImportTask",
        task_config_name="TheMediaTrustLpDetectionReportImportTaskConfig",
        scrum_team=cre,
        start_date=datetime(2024, 1, 23),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=["try changeField TheMediaTrustLpDetectionReportImportTask.UseReadonlyProvDb.Enabled true"]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ChinaCreativeApprovalTask",
        task_config_name="ChinaCreativeApprovalTaskConfig",
        scrum_team=cre,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=30),
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
        dag_tsg="https://thetradedesk.atlassian.net/wiki/x/LYUqGQ",
        task_execution_timeout=timedelta(hours=5),  # SWAT-18954
        telnet_commands=[
            "try changeField TencentGdtCreativeApprovalProvider.TencentGdtCreativeMigrationSwitch true",
            "try invokeMethod TtdConfiguration.AppSettings.Set ChinaCreativeApprovalTask.YouKu.Enabled true",
            "try invokeMethod DatabaseCreativeExternalApprovalDataService.BatchSizeOverride.set_Item 85 1000",
            "try changeField ChinaCreativeApprovalTask.UseReadonlyProvDb.Enabled true"
        ],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ChinaCreativeCopyToCNTask",
        task_config_name="ChinaCreativeCopyToCNTaskConfig",
        scrum_team=cre,
        start_date=datetime(2023, 11, 21),
        job_schedule_interval="*/20 * * * *",
        resources=TaskServicePodResources.large(),
        cloud_provider=alicloud,
        telnet_commands=["try changeField ChinaCreativeCopyToCNTask.UseReadonlyProvDb.Enabled true"]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoogleCreativePubSubCheckSubmittedCreativesTask",
        task_config_name="GooglePubSubCreativeCheckConfig",
        scrum_team=cre,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
