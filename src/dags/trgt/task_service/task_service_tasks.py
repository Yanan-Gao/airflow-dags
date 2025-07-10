from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import targeting

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GeoHighFrequencyPointsTask",
        scrum_team=targeting,
        task_config_name="GeoHighFrequencyPointsTaskConfig",
        start_date=datetime(2023, 8, 16),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=[
            "try changeField GeoHighFrequencyPointsTask.VerticaCluster 5",  # USEast
            "try changeField GeoHighFrequencyPointsTask.RowLimit 1",
            "try changeField GeoHighFrequencyPointsTask.BidReqPerDayCutoff 1000000000"
        ]
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GeoTileGenTask",
        scrum_team=targeting,
        task_config_name="GeoTileGenTaskConfig",
        start_date=datetime(2024, 12, 23),
        job_schedule_interval="0 */3 * * *",
        resources=TaskServicePodResources.medium()
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="GeoTilePushTask",
        scrum_team=targeting,
        task_config_name="GeoTilePushTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/2 * * * *",
        resources=TaskServicePodResources.medium()
    )
)
