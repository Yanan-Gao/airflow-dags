from datetime import datetime, timedelta
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import hpc

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="CacheDataGroupCountsTask",
        scrum_team=hpc,
        dag_tsg='https://thetradedesk.atlassian.net/wiki/x/VAACG',
        task_config_name="CacheDataGroupCountsTaskConfig",
        start_date=datetime(2023, 8, 12, 0, 0),
        job_schedule_interval=timedelta(days=1),
        resources=TaskServicePodResources.large(),
        task_execution_timeout=timedelta(hours=3),
        retries=3,
        configuration_overrides={
            "CacheDataGroupCounts.BatchSize": "100",
            "CacheDataGroupCounts.DataGroupSegmentLimit": "1000",
            "CacheDataGroupCounts.RunDateTime": '{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}',
            "AudienceCountsWebServiceProtocol": "https",
            "AudienceCountsWebServiceAddress": "batch-int.audience-count.adsrvr.org",
            "AudienceCountsWebServicePort": "9443",
            "AudienceCountsWalmartWebServiceAddress": "batch-int.audience-count.adsrvr.org",
            "AudienceCountsWalmartWebServicePort": "9443"
        }
    )
)
