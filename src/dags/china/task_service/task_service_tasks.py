from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.k8s_connection_helper import alicloud
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import china

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="UcfunnelChinaCreativeApprovalTask",
        task_config_name="UcfunnelChinaCreativeApprovalTaskConfig",
        scrum_team=china,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
        telnet_commands=["try changeField UcfunnelChinaCreativeApprovalTask.UseReadonlyProvDb.Enabled true"]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="FraudDataImportTask",
        task_config_name="FraudDataImportTaskConfig",
        scrum_team=china,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="UnionPayAudienceDataImportTask",
        task_config_name="UnionPayAudienceDataImportTaskConfig",
        scrum_team=china,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
        cloud_provider=alicloud,
    )
)
