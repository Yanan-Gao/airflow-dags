from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import OPATH

registry = TaskServiceDagRegistry(globals())

registry.register_dag(
    TaskServiceDagFactory(
        task_name="OpenPathReportScheduleSyncTask",
        scrum_team=OPATH.team,
        task_config_name="OpenPathReportScheduleSyncTaskConfig",
        start_date=datetime(2024, 3, 13),
        task_execution_timeout=timedelta(minutes=10),
        job_schedule_interval="*/5 * * * *",
        retries=0,
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/x/wwEfEQ",
    )
)
