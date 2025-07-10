from datetime import datetime
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.k8s_connection_helper import alicloud
from ttd.slack.slack_groups import bid

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdvertiserCorporationApprovalTask",
        task_config_name="AdvertiserCorporationApprovalTaskConfig",
        scrum_team=bid,
        start_date=datetime(2024, 1, 8),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="PullingBaiduPublisherSettingTask",
        task_config_name="PullingBaiduPublisherSettingTaskConfig",
        scrum_team=bid,
        start_date=datetime(2023, 11, 28),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SupplyVendorActivityReportTask",
        task_config_name="SupplyVendorActivityReportTaskConfig",
        scrum_team=bid,
        start_date=datetime(2023, 11, 27),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GenreCategoryAvailsUpdateTask",
        task_config_name="GenreCategoryAvailsUpdateTaskConfigCNWest01",
        task_name_suffix="CNWest01",
        scrum_team=bid,
        start_date=datetime(2024, 7, 8),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AdvertiserCorporationNotificationTask",
        task_config_name="AdvertiserCorporationNotificationTaskConfig",
        scrum_team=bid,
        start_date=datetime(2024, 7, 8),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
