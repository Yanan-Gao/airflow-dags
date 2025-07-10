from datetime import datetime

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import pdg

registry = TaskServiceDagRegistry(globals())

# Define the Airflow DAG using TaskServiceDagFactory
inventory_datarights_ingestion_task = TaskServiceDagFactory(
    task_name="InventoryDataRightsIngestionTask",
    task_config_name="InventoryDataRightsIngestionTaskConfig",
    scrum_team=pdg,
    start_date=datetime(2024, 10, 4),
    job_schedule_interval="0 3 * * *",
    resources=TaskServicePodResources.small(),  # Updated to use small resources
)

# Register the DAG using the registry for internal tracking purposes
registry.register_dag(inventory_datarights_ingestion_task)
