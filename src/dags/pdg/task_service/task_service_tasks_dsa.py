from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import pdg

registry = TaskServiceDagRegistry(globals())

registry.register_dag(
    TaskServiceDagFactory(
        task_name="DsaDefaultPayerNamePopulationTask",
        task_config_name="DsaDefaultPayerNamePopulationTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2024, 2, 20),
        job_schedule_interval="0 17 * * *",  # Scheduled once a day at 17:00 UTC
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="DsaPreciseGeoCalculationTask",
        task_config_name="DsaPreciseGeoCalculationTaskConfig",
        task_data=
        """{"LastSuccessStartTime": "{{ prev_start_date_success.isoformat() if prev_start_date_success.isoformat is defined else '' }}" }""",
        scrum_team=pdg,
        start_date=datetime(2024, 3, 25),
        job_schedule_interval=timedelta(hours=6),
        resources=TaskServicePodResources.medium(),
    )
)
