from datetime import datetime, timedelta

# from airflow import DAG

from ttd.task_service.k8s_connection_helper import alicloud
from ttd.task_service.task_service_dag import (
    TaskServiceDagFactory,
    TaskServiceDagRegistry,
)
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import hpc, dist

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="CookieMappingValidatorTask",
        scrum_team=hpc,
        task_config_name="CookieMappingValidatorTaskConfigPrimary",
        start_date=datetime.now() - timedelta(hours=3),
        telnet_commands=[
            "try invokeMethod CookieMappingValidatorTask.ForceSuccessPartners.Add rubicon",
            "try invokeMethod CookieMappingValidatorTask.ForceSuccessPartners.Add semasio",
            "try invokeMethod CookieMappingValidatorTask.ForceSuccessPartners.Add x2e7tq8",
            "try invokeMethod CookieMappingValidatorTask.ForceSuccessPartners.Add liveramp",
            "try invokeMethod CookieMappingValidatorTask.ForceSuccessPartners.Add i-behavior",
        ],
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdvertiserDataImportTask",
        scrum_team=hpc,
        task_config_name="AdvertiserDataImportTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportTask",
        scrum_team=hpc,
        task_config_name="ThirdPartyDataImportTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.custom(
            request_cpu="2",
            request_memory="4Gi",
            request_ephemeral_storage="15Gi",
            limit_memory="8Gi",
            limit_ephemeral_storage="30Gi",
        ),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TargetingDataLimitsUpdateTask",
        scrum_team=hpc,
        task_config_name="TargetingDataLimitsUpdateTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="RefreshTargetingDataHitCountsTask",
        scrum_team=hpc,
        task_config_name="RefreshTargetingDataHitCountsConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="UniversalPixelBackfillTask",
        scrum_team=hpc,
        task_config_name="UniversalPixelBackfillTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */8 * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="UniversalPixelBackfillV2Task",
        scrum_team=hpc,
        task_config_name="UniversalPixelBackfillV2TaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="IPTargetingDataImportTask",
        scrum_team=hpc,
        task_config_name="IPTargetingDataImportTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        task_execution_timeout=timedelta(hours=2),
        job_schedule_interval=timedelta(minutes=15),
        resources=TaskServicePodResources.custom(
            request_cpu="2",
            request_memory="4Gi",
            request_ephemeral_storage="15Gi",
            limit_memory="8Gi",
            limit_ephemeral_storage="30Gi",
        ),
        max_active_runs=5,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AlgoliaTracerBullet1PdTask",
        task_config_name="AlgoliaTracerBullet1PdTaskConfig",
        scrum_team=hpc,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/5 * * * *",
        task_name_suffix="prod_08",
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "AlgoliaTracerBullet1PdTask.Index": "1pd-cdc-prod-08",
            "AlgoliaTracerBullet1PdTask.TBSFileKeyPrefix": "firstPartyData/prod_08/",  # add prodTest/ when testing
            "AlgoliaTracerBullet1PdTask.MetricLabel": "prod_08",
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AlgoliaTracerBullet3PdTask",
        task_config_name="AlgoliaTracerBullet3PdTaskConfig",
        scrum_team=hpc,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        task_name_suffix="country-counts-07",
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "AlgoliaTracerBullet3PdTask.Index": "3pd-cdc-prod-provdb-country-counts-07",
            "AlgoliaTracerBullet3PdTask.ThirdPartyDataId": "17907088",
            "AlgoliaTracerBullet3PdTask.AlgoliaApplicationId": "YT7737MZO9",
            "AlgoliaTracerBullet3PdTask.AlgoliaApiSearchKey": "6f66d8a87f2ba9e76e8de601b0a6356e",
            "AlgoliaTracerBullet3PdTask.ProviderElementId": "secondaryTracerBullet",
            "AlgoliaTracerBullet3PdTask.ProviderId": "eltorotel",
            "AlgoliaTracerBullet3PdTask.TBSFileKeyPrefix": "thirdPartyData/countryCounts07/",  # add prodTest/ when testing
            "AlgoliaTracerBullet3PdTask.MetricLabel": "counts_07",
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AlgoliaTracerBullet3PdTask",
        task_config_name="AlgoliaTracerBullet3PdTaskConfig",
        scrum_team=hpc,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        task_name_suffix="country-counts-05",
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "AlgoliaTracerBullet3PdTask.Index": "3pd-cdc-prod-provdb-country-counts-05",
            "AlgoliaTracerBullet3PdTask.ThirdPartyDataId": "17907084",
            "AlgoliaTracerBullet3PdTask.ProviderElementId": "prodTracerBullet",
            "AlgoliaTracerBullet3PdTask.ProviderId": "eltorotel",
            "AlgoliaTracerBullet3PdTask.TBSFileKeyPrefix": "thirdPartyData/countryCounts05/",  # add prodTest/ when testing
            "AlgoliaTracerBullet3PdTask.MetricLabel": "counts_05",
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AlgoliaTracerBullet3PdRatesTask",
        task_config_name="AlgoliaTracerBullet3PdRatesTaskConfig",
        scrum_team=dist,
        alert_channel="tf-rates-and-fees-alerts",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        task_name_suffix="country-counts-05",
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[dist.jira_team],
        configuration_overrides={
            "AlgoliaTracerBullet3PdRatesTask.Index": "3pd-cdc-prod-provdb-country-counts-05",
            "AlgoliaTracerBullet3PdRatesTask.ProviderId": "eltorotel",
            "AlgoliaTracerBullet3PdRatesTask.ProviderElementId": "DataRatesTracerSegment",
            "AlgoliaTracerBullet3PdRatesTask.ThirdPartyDataId": "17933996",
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AlgoliaTracerBullet3PdRatesTask",
        task_config_name="AlgoliaTracerBullet3PdRatesTaskConfig",
        scrum_team=dist,
        alert_channel="tf-rates-and-fees-alerts",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        task_name_suffix="rates-cdc",
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[dist.jira_team],
        configuration_overrides={
            "AlgoliaTracerBullet3PdRatesTask.Index": "3pd-cdc-prod-provdb-rates-01-test",
            "AlgoliaTracerBullet3PdRatesTask.ProviderId": "eltorotel",
            "AlgoliaTracerBullet3PdRatesTask.ProviderElementId": "DataRatesTracerSegment-RatesCDC",
            "AlgoliaTracerBullet3PdRatesTask.ThirdPartyDataId": "17933997",
            "AlgoliaTracerBullet3PdRatesTask.MetricLabel": "rates-cdc",
            "AlgoliaTracerBullet3PdRatesTask.TBSFileKeyPrefix": "thirdPartyDataRatesRates/rates-cdc/"
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AutomatedTargetingDataRemovalTask",
        task_config_name="AutomatedTargetingDataRemovalTaskConfig",
        scrum_team=dist,
        alert_channel="tf-rates-and-fees-alerts",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=10),
        resources=TaskServicePodResources.medium(),
        teams_allowed_to_access=[dist.jira_team],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TargetingDataSwapsTask",
        task_config_name="TargetingDataSwapsTaskConfig",
        scrum_team=hpc,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=10),
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "TargetingDataSwapsTask.AdGroupsBatchSize": "5000",
            "TargetingDataSwapsTask.SegmentsBatchSize": "1000",
            "TargetingDataSwapsTask.SqlCommandExecutionTimeOut": "180",
            "TargetingDataSwapsTask.RetriesMax": "10",
        },
    )
)
china_registry = TaskServiceDagRegistry(globals())
china_registry.register_dag(
    TaskServiceDagFactory(
        task_name="CookieMappingValidatorTask",
        scrum_team=hpc,
        task_config_name="CookieMappingValidatorTaskConfigChina",
        task_name_suffix="China",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/5 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
china_registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportTask",
        scrum_team=hpc,
        task_config_name="ThirdPartyDataImportTaskConfig",
        task_name_suffix="China",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "ThirdPartyDataImport.FolderSuffix": "china",
            "ThirdPartyDataImport.IsUserIdAlreadyHashed": "true",
            "ThirdPartyDataImport.DefaultDataCenterId": "12",
        },
        cloud_provider=alicloud,
    )
)
china_registry.register_dag(
    TaskServiceDagFactory(
        task_name="IPTargetingDataImportTask",
        scrum_team=hpc,
        task_config_name="IPTargetingDataImportTaskConfigChina",
        task_name_suffix="China",
        start_date=datetime.now() - timedelta(hours=3),
        task_execution_timeout=timedelta(hours=2),
        job_schedule_interval=timedelta(minutes=15),
        cloud_provider=alicloud,
        resources=TaskServicePodResources.custom(
            request_cpu="2",
            request_memory="4Gi",
            request_ephemeral_storage="15Gi",
            limit_memory="8Gi",
            limit_ephemeral_storage="30Gi",
        ),
    )
)
