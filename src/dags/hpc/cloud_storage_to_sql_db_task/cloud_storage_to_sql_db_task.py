from datetime import timedelta
from typing import List, Dict, Any, Optional

from airflow.operators.python import PythonOperator

from dags.hpc.cloud_storage_to_sql_db_task.utils import CloudStorageToSqlColumn, get_dataset_prefix, \
    column_mapping_to_string, ConsistencyCheckCondition, convert_bool_to_config_str, \
    consistency_check_conditions_to_string
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.datasets.dataset import Dataset
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import SlackTeam
from ttd.task_service.k8s_pod_resources import TaskServicePodResources, PodResources
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask

task_service_task_name = "CloudStorageToSqlDbDataTransferTask"

default_staging_database_name = "TempStore"
default_staging_table_schema_name = "tdu"
default_insert_to_staging_table_batch_size = 10000
default_insert_to_staging_table_timeout = 600
default_upsert_to_destination_table_batch_size = 10000
default_upsert_to_destination_table_batch_delay = 0.5
default_upsert_to_destination_table_timeout = 1800
default_upsert_to_destination_table_retries = 2


def create_cloud_storage_to_sql_db_task(
    name: str,
    scrum_team: SlackTeam,
    dataset_name: str,
    storage_bucket: str,
    storage_key_prefix: str,
    destination_database_name: str,
    destination_table_schema_name: str,
    destination_table_name: str,
    column_mapping: List[CloudStorageToSqlColumn],
    staging_database_name: str = default_staging_database_name,
    staging_table_schema_name: str = default_staging_table_schema_name,
    cloud_storage_provider: CloudProvider = CloudProviders.aws,
    insert_to_staging_table_batch_size: int = default_insert_to_staging_table_batch_size,
    insert_to_staging_table_timeout: int = default_insert_to_staging_table_timeout,
    upsert_to_destination_table_batch_size: int = default_upsert_to_destination_table_batch_size,
    upsert_to_destination_table_batch_delay: float = default_upsert_to_destination_table_batch_delay,
    upsert_to_destination_table_timeout: int = default_upsert_to_destination_table_timeout,
    upsert_to_destination_table_retries: int = default_upsert_to_destination_table_retries,
    task_execution_timeout: Optional[timedelta] = None,
    ensure_consistency_between_source_and_destination: bool = False,
    consistency_check_based_on_column_values: Optional[List[ConsistencyCheckCondition]] = None,
    branch_name: Optional[str] = None,
    use_sandbox: bool = False,
    resources: PodResources = TaskServicePodResources().medium(),
    use_partition_switch: bool = False,
    task_retries: int = 0
) -> OpTask:
    if cloud_storage_provider != CloudProviders.aws:
        raise NotImplementedError("Only AWS S3 is supported at the moment")
    if task_execution_timeout is None:
        # Time-out for the delete script is the same as insert_to_staging_table_timeout
        timeout_for_deletes = int(ensure_consistency_between_source_and_destination) * insert_to_staging_table_timeout
        task_execution_timeout = timedelta(
            seconds=(upsert_to_destination_table_timeout + insert_to_staging_table_timeout + timeout_for_deletes)
        )
    if task_execution_timeout.total_seconds() < insert_to_staging_table_timeout:
        raise ValueError(
            f"The provided task_execution_timeout ({task_execution_timeout.total_seconds()} seconds) should be "
            f">= the provided insert_to_staging_table_timeout ({insert_to_staging_table_timeout} seconds)."
        )
    if task_execution_timeout.total_seconds() < upsert_to_destination_table_timeout:
        raise ValueError(
            f"The provided task_execution_timeout ({task_execution_timeout.total_seconds()} seconds) should be "
            f">= the provided upsert_to_destination_table_timeout ({upsert_to_destination_table_timeout} seconds)"
        )
    if use_partition_switch and destination_database_name != staging_database_name:
        raise ValueError(
            f"The provided destination database ({destination_database_name}) should be "
            f"== to the provided staging database ({staging_database_name}) when use_partition_switch is true."
        )
    converted_consistency_check_conditions = "" if consistency_check_based_on_column_values is None else consistency_check_conditions_to_string(
        consistency_check_based_on_column_values
    )

    task_service_operator = TaskServiceOperator(
        task_name=task_service_task_name,
        scrum_team=scrum_team,
        task_name_suffix=name,
        branch_name=branch_name,
        resources=resources,
        retries=task_retries,
        configuration_overrides={
            "CloudStorageToSqlDbDataTransferTask.CloudStorageDatasetName":
            dataset_name,
            "CloudStorageToSqlDbDataTransferTask.CloudStorageDatasetBucket":
            storage_bucket,
            "CloudStorageToSqlDbDataTransferTask.CloudStorageDatasetPrefix":
            storage_key_prefix,
            "CloudStorageToSqlDbDataTransferTask.SqlDbDestinationDatabaseName":
            destination_database_name,
            "CloudStorageToSqlDbDataTransferTask.SqlDbStagingDatabaseName":
            staging_database_name,
            "CloudStorageToSqlDbDataTransferTask.SqlDbDestinationTableSchemaName":
            destination_table_schema_name,
            "CloudStorageToSqlDbDataTransferTask.SqlDbStagingTableSchemaName":
            staging_table_schema_name,
            "CloudStorageToSqlDbDataTransferTask.SqlDbDestinationTableName":
            destination_table_name,
            "CloudStorageToSqlDbDataTransferTask.FileToSqlColumnMappings":
            column_mapping_to_string(column_mapping),
            "CloudStorageToSqlDbDataTransferTask.InsertToStagingBatchSize":
            str(insert_to_staging_table_batch_size),
            "CloudStorageToSqlDbDataTransferTask.InsertToStagingTimeout":
            str(insert_to_staging_table_timeout),
            "CloudStorageToSqlDbDataTransferTask.UpsertToDestinationBatchSize":
            str(upsert_to_destination_table_batch_size),
            "CloudStorageToSqlDbDataTransferTask.UpsertToDestinationBatchDelayInSeconds":
            str(upsert_to_destination_table_batch_delay),
            "CloudStorageToSqlDbDataTransferTask.UpsertToDestinationTimeout":
            str(upsert_to_destination_table_timeout),
            "CloudStorageToSqlDbDataTransferTask.UpsertToDestinationMaxRetryCount":
            str(upsert_to_destination_table_retries),
            "CloudStorageToSqlDbDataTransferTask.EnsureConsistencyBetweenSourceAndDestination":
            convert_bool_to_config_str(ensure_consistency_between_source_and_destination),
            "CloudStorageToSqlDbDataTransferTask.ConsistencyCheckBasedOnColumnValues":
            converted_consistency_check_conditions,
            "CloudStorageToSqlDbDataTransferTask.UseSandbox":
            convert_bool_to_config_str(use_sandbox),
            "CloudStorageToSqlDbDataTransferTask.UsePartitionSwitch":
            convert_bool_to_config_str(use_partition_switch)
        },
        task_execution_timeout=task_execution_timeout
    )

    return OpTask(op=task_service_operator)


def create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name: str,
    scrum_team: SlackTeam,
    dataset: Dataset,
    dataset_partitioning_args: Dict[str, Any],
    destination_database_name: str,
    destination_table_schema_name: str,
    destination_table_name: str,
    column_mapping: List[CloudStorageToSqlColumn],
    staging_database_name: str = default_staging_database_name,
    staging_table_schema_name: str = default_staging_table_schema_name,
    cloud_storage_provider: CloudProvider = CloudProviders.aws,
    insert_to_staging_table_batch_size: int = default_insert_to_staging_table_batch_size,
    insert_to_staging_table_timeout: int = default_insert_to_staging_table_timeout,
    upsert_to_destination_table_batch_size: int = default_upsert_to_destination_table_batch_size,
    upsert_to_destination_table_batch_delay: float = default_upsert_to_destination_table_batch_delay,
    upsert_to_destination_table_timeout: int = default_upsert_to_destination_table_timeout,
    upsert_to_destination_table_retries: int = default_upsert_to_destination_table_retries,
    task_execution_timeout: Optional[timedelta] = None,
    ensure_consistency_between_source_and_destination: bool = False,
    consistency_check_based_on_column_values: Optional[List[ConsistencyCheckCondition]] = None,
    branch_name: Optional[str] = None,
    use_sandbox: bool = False,
    resources: PodResources = TaskServicePodResources().medium(),
    use_partition_switch: bool = False,
    task_retries: int = 0
) -> ChainOfTasks:
    dataset = dataset.with_cloud(cloud_storage_provider)
    dataset_name = dataset.data_name
    storage_bucket = dataset.bucket

    get_dataset_prefix_task_name = f'{name}-get_dataset_prefix_task'
    get_dataset_prefix_task = OpTask(
        op=PythonOperator(
            task_id=get_dataset_prefix_task_name,
            python_callable=get_dataset_prefix,
            op_kwargs={
                'dataset': dataset,
                'dataset_partitioning_args': dataset_partitioning_args
            }
        )
    )
    storage_key_prefix = f"{{{{ task_instance.xcom_pull('{get_dataset_prefix_task_name}') }}}}"

    task_service_task = create_cloud_storage_to_sql_db_task(
        name=name,
        scrum_team=scrum_team,
        dataset_name=dataset_name,
        storage_bucket=storage_bucket,
        storage_key_prefix=storage_key_prefix,
        destination_database_name=destination_database_name,
        destination_table_schema_name=destination_table_schema_name,
        destination_table_name=destination_table_name,
        column_mapping=column_mapping,
        staging_database_name=staging_database_name,
        staging_table_schema_name=staging_table_schema_name,
        cloud_storage_provider=cloud_storage_provider,
        insert_to_staging_table_batch_size=insert_to_staging_table_batch_size,
        insert_to_staging_table_timeout=insert_to_staging_table_timeout,
        task_execution_timeout=task_execution_timeout,
        upsert_to_destination_table_batch_size=upsert_to_destination_table_batch_size,
        upsert_to_destination_table_batch_delay=upsert_to_destination_table_batch_delay,
        upsert_to_destination_table_timeout=upsert_to_destination_table_timeout,
        upsert_to_destination_table_retries=upsert_to_destination_table_retries,
        ensure_consistency_between_source_and_destination=ensure_consistency_between_source_and_destination,
        consistency_check_based_on_column_values=consistency_check_based_on_column_values,
        branch_name=branch_name,
        use_sandbox=use_sandbox,
        resources=resources,
        use_partition_switch=use_partition_switch,
        task_retries=task_retries
    )

    chained_tasks = ChainOfTasks(task_id=f'{name}-tasks', tasks=[get_dataset_prefix_task, task_service_task])

    return chained_tasks
