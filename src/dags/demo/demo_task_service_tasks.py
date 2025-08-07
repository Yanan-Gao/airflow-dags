from datetime import datetime, timedelta

from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
    PersistentStorageType,
)
from ttd.task_service.task_service_dag import (
    TaskServiceDagFactory,
    TaskServiceDagRegistry,
)
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.k8s_connection_helper import azure
from ttd.slack.slack_groups import dataproc

# All the following examples run the task defined here:
# https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/TaskExecution/TaskService/TTD.Domain.TaskExecution.TaskService/Tasks/TestTasks/EmptyTask.cs#L18
registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    # A task that runs every hour, at the top of the hour.
    TaskServiceDagFactory(
        task_name="EmptyTask",
        scrum_team=dataproc,
        task_config_name="EmptyTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)
registry.register_dag(
    # Runs the task every day at midnight in Azure K8s:
    TaskServiceDagFactory(
        task_name="EmptyTask",
        scrum_team=dataproc,
        task_config_name="EmptyTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 0 * * *",
        task_name_suffix="azure",  # This distinguishes tasks that share a task_name from each other
        cloud_provider=azure,
        resources=TaskServicePodResources.medium(),
        dag_tags=["Demo"],  # Optional parameter to add tags to DAG in the Airflow UI
    )
)

registry.register_dag(
    # Runs the task with specified higher resources and a longer timeout for execution:
    TaskServiceDagFactory(
        task_name="EmptyTask",
        scrum_team=dataproc,
        task_name_suffix="custom-resources",
        task_config_name="EmptyTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        task_execution_timeout=timedelta(hours=5),
        resources=TaskServicePodResources.custom(
            request_cpu="3",
            request_memory="6Gi",
            request_ephemeral_storage="15Gi",
            limit_memory="12Gi",
            limit_ephemeral_storage="35Gi",
        ),
        dag_tags=["Demo"],
    )
)
registry.register_dag(
    # Runs the task with a telnet command and an override to a value in ConfigurationOverrides.yaml:
    TaskServiceDagFactory(
        task_name="EmptyTask",
        scrum_team=dataproc,
        task_name_suffix="custom-commands",
        task_config_name="EmptyTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=["try invokeMethod EmptyTask.MyPublicMethod TheParameter"],
        configuration_overrides={
            "EmptyTask.KeyInConfigOverrides": "TheNewValue",
        },
        dag_tags=["Demo"],
    )
)

registry.register_dag(
    # Runs the task with one-pod persistent storage type
    TaskServiceDagFactory(
        task_name="EmptyPersistentStorageTask",
        scrum_team=dataproc,
        task_name_suffix="one-pod-persistent-storage",
        task_config_name="EmptyTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 * * * *",
        resources=TaskServicePodResources.medium(),
        persistent_storage_config=PersistentStorageConfig(PersistentStorageType.ONE_POD, "20Gi"),
    )
)
