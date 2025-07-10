from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, create_all_for_vertica_variant_group, \
    TaskServiceDagRegistry
from ttd.slack.slack_groups import chnl
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.vertica_clusters import TaskVariantGroup

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdSquareSyncTask",
        task_config_name="AdSquareSyncTaskConfig",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/120 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AppNexusReportServiceTask",
        task_config_name="AppNexusReportServiceTaskConfig",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */4 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DOOHAdvertiserExportTask",
        task_config_name="DOOHAdvertiserExportTaskConfig",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        # Discouraged but no easy way for a cron to represent "every 45 minutes"
        job_schedule_interval=timedelta(minutes=45),
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField DOOHAdvertiserExportTask.QueryTimeoutInSeconds 1200",  # 20 minutes
        ]
    )
)

# This batch will run for 'USE_AWS', 'or2', 'va6', 'wa1', 'vaf', 'vad', 'vae', 'ny3', 'va9p', 'va9p3','vam', 'JP1', 'sg2'
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DOOHPushLocationTargetingToAerospikeTask",
        task_config_name="DOOHPushLocationTargetingToAerospikeTaskConfig",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=15),  # Scheduled for every 15 mins, but can take 1-2 hours
        task_execution_timeout=timedelta(hours=3),
        resources=TaskServicePodResources.custom(
            request_cpu="8", request_memory="128Gi", request_ephemeral_storage="15Gi", limit_memory="256Gi", limit_ephemeral_storage="60Gi"
        ),
        configuration_overrides={
            "DOOHPushLocationTargetingToAerospikeTask.ExportTempDirForResultFiles":
            "/tmp/dooh/at",
            "DOOHPushLocationTargetingToAerospikeTask.AerospikeExcludeInstances":
            "['IE1','DE2','UsWest', 'UsEast', 'EuWest', 'APac', 'Tokyo', 'Shanghai', 'Virginia', 'CA2', 'CN1', 'CN2', 'ca4', 'ny1', 'br1', 'de1', 'CN3', 'CN4', 'CN5', 'CN6', 'CN7', 'va9', 'hk3', 'vat']"
        },
        telnet_commands=[
            "try changeField DOOHPushLocationTargetingToAerospikeTask.MaxDegreeOfParallelismPuts 300",
        ]
    )
)

# This batch will run for 'IE1','DE2', 'UsEast', 'APac', 'Tokyo', 'Shanghai', 'Virginia', 'br1', 'va9', 'ny1'
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DOOHPushLocationTargetingToAerospikeTask",
        task_config_name="DOOHPushLocationTargetingToAerospikeTaskConfig",
        task_name_suffix="batch2",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=15),  # Scheduled for every 15 mins, but can take 1-2 hours
        task_execution_timeout=timedelta(hours=3),
        resources=TaskServicePodResources.custom(
            request_cpu="8", request_memory="128Gi", request_ephemeral_storage="15Gi", limit_memory="256Gi", limit_ephemeral_storage="60Gi"
        ),
        configuration_overrides={
            "DOOHPushLocationTargetingToAerospikeTask.ExportTempDirForResultFiles":
            "/tmp/dooh/at",
            "DOOHPushLocationTargetingToAerospikeTask.AerospikeExcludeInstances":
            "['UsWest', 'EuWest', 'CA2', 'CN1', 'CN2', 'de1', 'CN3', 'CN4', 'CN5', 'CN6', 'CN7', 'USE_AWS', 'or2', 'va6', 'JP1', 'hk3', 'wa1', 'vaf', 'vad', 'vae', 'ny3', 'va9p', 'sg2', 'va9p3','vam', 'vat','ca4']"
        },
        telnet_commands=[
            "try changeField DOOHPushLocationTargetingToAerospikeTask.MaxDegreeOfParallelismPuts 300",
        ]
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="DOOHPushArpTiersToAerospikeTask",
        task_config_name="DOOHPushArpTiersToAerospikeConfig",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=15),
        resources=TaskServicePodResources.custom(
            request_cpu="4", request_memory="64Gi", request_ephemeral_storage="15Gi", limit_memory="128Gi", limit_ephemeral_storage="60Gi"
        ),
        configuration_overrides={
            "DOOHPushArpTiersToAerospikeTask.ExportTempDirForResultFiles": "/tmp/dooh/arp",
            "DOOHPushArpTiersToAerospikeTask.AerospikeExcludeInstances": "['CN3','CN4','CN5','USWest', 'EuWest','HK3', 'DE1','vat','ca4']"
        },
        telnet_commands=[
            "try changeField DOOHPushArpTiersToAerospikeTask.MaxHoursToLookInPastForData 72",
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DoubleVerifyDomainPullTask",
        task_config_name="DoubleVerifyDomainPullTaskConfig",
        scrum_team=chnl,
        start_date=datetime(2023, 9, 1),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SimilarWebAppMetadataImportTask",
        task_config_name="SimilarWebAppMetadataImportTaskConfig",
        scrum_team=chnl,
        start_date=datetime(2023, 9, 1),
        job_schedule_interval="0 12 1,16 * *",
        resources=TaskServicePodResources.medium(),
        task_execution_timeout=timedelta(hours=8)
    )
)
registry.register_dags(
    create_all_for_vertica_variant_group(
        vertica_variant_group=TaskVariantGroup.VerticaAws,
        task_name="PrivateContractWinRateUpdateTask",
        task_config_name="PrivateContractWinRateUpdateTaskConfig",
        scrum_team=chnl,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DOOHWriteKpiMetricsTask",
        task_config_name="DOOHWriteKpiMetricsTaskConfig",
        scrum_team=chnl,
        job_schedule_interval="0 * * * *",
        start_date=datetime.now() - timedelta(hours=3),
        resources=TaskServicePodResources.small()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DOOHThirdPartyDataAuditingTask",
        task_config_name="DOOHThirdPartyDataAuditingTaskConfig",
        scrum_team=chnl,
        start_date=datetime(2024, 1, 1),
        job_schedule_interval="0 13 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AppleEnterprisePartnerFeedImportTask",
        task_config_name="AppleEnterprisePartnerFeedImportTaskConfig",
        scrum_team=chnl,
        start_date=datetime(2024, 8, 1),
        job_schedule_interval="0 6 */1 * *",
        resources=TaskServicePodResources.custom(request_cpu="1", request_memory="2Gi", limit_memory="4Gi", limit_ephemeral_storage="5Gi"),
    )
)
