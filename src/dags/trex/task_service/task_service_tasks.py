from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.persistent_storage.persistent_storage_config import PersistentStorageConfig, PersistentStorageType
from ttd.slack.slack_groups import trex

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="RollupActivityLogsTask",
        task_config_name="RollupActivityLogsConfig",
        scrum_team=trex,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=2),  # Scheduled for every 2 mins, but can take more time
        task_execution_timeout=timedelta(hours=4),
        resources=TaskServicePodResources
        .custom(request_cpu="6", request_memory="5Gi", limit_memory="6Gi", limit_ephemeral_storage="500Mi"),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.MANY_PODS, "40Gi", base_name="ActivityLog"),
        telnet_commands=[
            "try changeField PassThroughFeeCardRollupHandler.EnablePassThroughFeeCardProcessing.Enabled true",  # Enabled Activity Rollup for PassThroughFees
            "change AdGroupRollupHandler.EnableAdGroupCreativeProcessing.Enabled true",  # Enabled Activity Rollup for Creatives
            "change BidListRollupHandler.EnableMarketplaceNameGql.Enabled true",  # Enable MarketplaceGQL call for marketplace changes
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="BidLineRollupActivityLogsTask",
        task_config_name="BidLineRollupActivityLogsConfig",
        scrum_team=trex,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=2),  # Scheduled for every 2 mins, but can take more time
        task_execution_timeout=timedelta(hours=4),
        resources=TaskServicePodResources
        .custom(request_cpu="8", request_memory="3Gi", limit_memory="4Gi", limit_ephemeral_storage="500Mi"),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.MANY_PODS, "40Gi", base_name="ActivityLog")
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SystemBidLineRollupActivityLogsTask",
        task_config_name="SystemBidLineRollupActivityLogsConfig",
        scrum_team=trex,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=2),  # Scheduled for every 2 mins, but can take more time
        resources=TaskServicePodResources
        .custom(request_cpu="21", request_memory="2Gi", limit_memory="3Gi", limit_ephemeral_storage="500Mi"),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.MANY_PODS, "40Gi", base_name="ActivityLog")
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoodwayGroupRollupActivityLogsTask",
        task_config_name="GoodwayGroupRollupActivityLogsConfig",
        scrum_team=trex,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=2),  # Scheduled for every 2 mins, but can take more time
        resources=TaskServicePodResources
        .custom(request_cpu="2", request_memory="1Gi", limit_memory="1.5Gi", limit_ephemeral_storage="500Mi"),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.MANY_PODS, "40Gi", base_name="ActivityLog")
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GoodwayGroupBidLineRollupActivityLogsTask",
        task_config_name="GoodwayGroupBidLineRollupActivityLogsConfig",
        scrum_team=trex,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=2),  # Scheduled for every 2 mins, but can take more time
        resources=TaskServicePodResources
        .custom(request_cpu="2", request_memory="1.25Gi", limit_memory="1.75Gi", limit_ephemeral_storage="500Mi"),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.MANY_PODS, "40Gi", base_name="ActivityLog")
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ActivityLogFileRecoveryTask",
        task_config_name="ActivityLogFileRecoveryTaskConfig",
        scrum_team=trex,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=timedelta(minutes=2),  # Scheduled for every 2 mins, but can take more time
        task_execution_timeout=timedelta(hours=12),
        resources=TaskServicePodResources
        .custom(request_cpu="12", request_memory="48Gi", limit_memory="48Gi", limit_ephemeral_storage="500Mi"),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.MANY_PODS, "40Gi", base_name="ActivityLog")
    )
)
