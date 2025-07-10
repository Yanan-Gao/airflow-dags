from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import mqe
from ttd.task_service.vertica_clusters import VerticaCluster

dag_begin_date_str = "{{ data_interval_start.isoformat() }}"
registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FraudTask",
        task_config_name="FraudTaskConfig",
        scrum_team=mqe,
        start_date=datetime(2024, 7, 18, 14, 30),
        vertica_cluster=VerticaCluster.USEast01,
        task_execution_timeout=timedelta(hours=2),
        job_schedule_interval=timedelta(days=1),
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 360 Newsweek",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 387 Okezone",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 388 MingPao",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 389 InfolinksMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 390 Overwolf",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 391 Nitro",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 392 LoveToKnow",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 393 AncestryCOM",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 394 InternetBrands",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 395 DirectionDev",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 396 InvestingChannel",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 397 VoxMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 398 TimeOutGroup",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 399 RealtorCOM",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 400 DowJones",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 401 ZoomerMediaLtd",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 402 CineplexMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 403 M32Connect",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 404 CanadianBroadcastingCo",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 405 TheAtlantic",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 406 NetworldTechLtd",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 407 GRVMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 414 Thanhnien",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 415 Networld",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 416 ZeamMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 417 Viva",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 418 SavageVentures",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 419 IXL",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 420 Traffective",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 421 LongitudeAds",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 422 StaticMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 423 FanDroppings",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 424 Gumtree",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 425 DangerTV",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 426 DigitalBloom",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 427 WBD",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 428 FuelDigitalMedia",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 429 TownSquare",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 430 Footballco",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 431 YourTango",
            "invoke SupplyVendorInformationHolder.Telnet_AddSupplyVendor_OpenPath 432 Learfield",
        ],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SellersJsonDomainScannerTask",
        task_config_name="SellersJsonDomainScannerTaskConfig",
        scrum_team=mqe,
        start_date=datetime(2024, 7, 11),
        job_schedule_interval="0 */4 * * *",
        task_execution_timeout=timedelta(hours=2),
        resources=TaskServicePodResources.custom("1", "8Gi", "16Gi", "1Gi", "1Gi"),
        telnet_commands=["try changeField SellersJsonDomainScanner.UseUTF8Encoding true"],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SimilarWebDomainImportTask",
        task_config_name="SimilarWebDomainImportTaskConfig",
        scrum_team=mqe,
        start_date=datetime(2023, 11, 28),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="PlatformSPOSyntheticDealTask",
        scrum_team=mqe,
        task_config_name="PlatformSPOSyntheticDealTaskConfig",
        start_date=datetime(2023, 11, 14),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        retries=0,
        configuration_overrides={"PlatformSPOSyntheticDealTask.RunForSelectedPublishersOnly": "false"},
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DeepSeeHighRiskSitesDataImportTask",
        scrum_team=mqe,
        task_config_name="DeepSeeHighRiskSitesDataImportTaskConfig",
        start_date=datetime(2024, 8, 21),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=2),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ProcessDeepSeeViolationsTask",
        scrum_team=mqe,
        task_config_name="ProcessDeepSeeViolationsTaskConfig",
        start_date=datetime(2024, 9, 4),
        job_schedule_interval="0 2 * * 1-5",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=8),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AppMetadataAutomatedEnforcementTask",
        scrum_team=mqe,
        task_config_name="AppMetadataAutomatedEnforcementTaskConfig",
        start_date=datetime(2024, 10, 22),
        job_schedule_interval="0 0 * * 1-5",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=8),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MarketplaceQualityPolicyAuditNotificationTask",
        scrum_team=mqe,
        task_config_name="MarketplaceQualityPolicyAuditNotificationTaskConfig",
        start_date=datetime(2024, 10, 1),
        job_schedule_interval="0 10 * * 1-5",
        resources=TaskServicePodResources.medium(),
        configuration_overrides={"MarketplaceQualityPolicyAuditNotificationTask.SendSlackMessage": "true"},
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AppMetadataImportTask",
        task_config_name="AppMetadataImportTaskConfig",
        scrum_team=mqe,
        start_date=datetime(2024, 4, 13),
        job_schedule_interval="0 */6 * * *",
        resources=TaskServicePodResources.custom("2", "8Gi", "16Gi", "4Gi", "4Gi"),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdsTxtDomainScannerTask",
        task_config_name="AdsTxtDomainScannerTaskConfig",
        scrum_team=mqe,
        start_date=datetime(2024, 1, 12),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "changeField AdsTxtManager.DecodeEncodedSpaces.Enabled true",
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoogleVendorsSyncTask",
        task_config_name="GoogleVendorsSyncTaskConfig",
        scrum_team=mqe,
        start_date=datetime(2024, 1, 12),
        job_schedule_interval="0 18 * * *",
        resources=TaskServicePodResources.medium(
        )  # From time to time the task may be on the heavy side. This is medium to support these instances.
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="JouncePropertyScoresDataImportTask",
        scrum_team=mqe,
        task_config_name="JouncePropertyScoresDataImportTaskConfig",
        start_date=datetime(2024, 11, 18),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.custom("2", "8Gi", "16Gi", "4Gi", "4Gi"),
        task_execution_timeout=timedelta(hours=2),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="JounceAutomatedEnforcementTask",
        scrum_team=mqe,
        task_config_name="JounceAutomatedEnforcementTaskConfig",
        start_date=datetime(2024, 11, 19),
        job_schedule_interval="0 2 * * 1-5",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=8),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SinceraBrandSafetyUploadTask",
        task_name_suffix="Hourly",
        scrum_team=mqe,
        task_config_name="SinceraBrandSafetyUploadTaskConfig",
        start_date=datetime(2024, 2, 26),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=1),
        configuration_overrides={
            "SinceraBrandSafetyUploadTask.AwsBucketPath": "914/inbound/",
            "SinceraBrandSafetyUploadTask.DagBeginDate": dag_begin_date_str,
            "SinceraBrandSafetyUploadTask.RunningHourly": "true"
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MarketplaceQualityOpenPathOnboardingTask",
        scrum_team=mqe,
        task_config_name="MarketplaceQualityOpenPathOnboardingTaskConfig",
        start_date=datetime(2024, 12, 19),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=1),
        configuration_overrides={"MarketplaceQualityOpenPathOnboardingTask.DatabaseBatchSize": "50"},
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MarketplaceQualityOpenPathOnboardingAuditNotificationTask",
        scrum_team=mqe,
        task_config_name="MarketplaceQualityOpenPathOnboardingAuditNotificationTaskConfig",
        start_date=datetime(2024, 12, 30),
        job_schedule_interval="0 17 * * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=1),
        configuration_overrides={"MarketplaceQualityOpenPathOnboardingAuditNotificationTask.SlackTagTrafficPctThreshold": "0.3"},
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="IdBridgingAutomatedEnforcementTask",
        scrum_team=mqe,
        task_config_name="IdBridgingAutomatedEnforcementTaskConfig",
        start_date=datetime(2025, 4, 4),
        job_schedule_interval="0 4 * * 1-5",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=1),
        configuration_overrides={
            "IdBridgingAutomatedEnforcementTask.BridgingRateThreshold": "0.0025",  # 0.25%
            "IdBridgingAutomatedEnforcementTask.SendSlackMessages": "true",
            "IdBridgingAutomatedEnforcementTask.WriteToIQABidFactors": "true",
            "IdBridgingAutomatedEnforcementTask.MinimumBridgedIds": "100",
            "IdBridgingAutomatedEnforcementTask.MinimumIQABidFactor": "0.25",
            "IdBridgingAutomatedEnforcementTask.MaximumIQABidFactor": "0.9",
            "IdBridgingAutomatedEnforcementTask.IQABidFactorLevel": "2",  # Platform
            "IdBridgingAutomatedEnforcementTask.FilterToSPOPublishers": "false"
        },
    )
)
