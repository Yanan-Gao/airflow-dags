from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import fineng

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AFCEngineTask",
        scrum_team=fineng,
        task_config_name="AFCEngineTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        configuration_overrides={"AFCEngine.DebugOutput": "true"},
        telnet_commands=["changeField GetJbpBillTimeCreditsTask.RunGetJbpBillTimeCreditsTask.Enabled true"],
        task_execution_timeout=timedelta(hours=3),
        retries=0,
        enable_slack_alert=False
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GetQualifiedCustomersAndAdvertisersForPaymentDiscountTask",
        scrum_team=fineng,
        task_config_name="GetQualifiedCustomersAndAdvertisersForPaymentDiscountTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SendInvoiceSubmissionReportTask",
        scrum_team=fineng,
        task_config_name="SendInvoiceSubmissionReportTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 23 * * *"
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="SupplyVendorReportingTask",
        scrum_team=fineng,
        task_config_name="SupplyVendorReportingTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
