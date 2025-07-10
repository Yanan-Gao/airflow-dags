from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from pendulum import DateTime, now

from ttd.ttdenv import TtdEnv, TtdEnvFactory


def get_is_prod_enviroment() -> bool:
    job_environment: TtdEnv = TtdEnvFactory.get_from_system()
    return job_environment == TtdEnvFactory.prod


S3_DEST_URI_BUCKET_KEY = 's3_dest_uri_bucket'
S3_DEST_URI_PREFIX_KEY = 's3_uri_prefix'
S3_DEST_URI_KEY = 's3_dest_uri'
PROVISIONING_SQL_CONNECTION = 'provdb_inventory_selection'
PROVISIONING_DB_NAME = 'Provisioning'
CREATE_VERSION_TASK_ID = 'create_version_task'
VERSION_ID_KEY = 'version_id'
INVMKT_TEAM = 'INVMKT'
INVMKT_ALARM_SLACK_CHANNEL = 'scrum-invmkt-alarms'
BUILD_S3_DESTINATION_URI_TASK_ID = 'build_s3_destination_uri'
SET_VERSION_TO_COMPLETE_TASK = 'set_version_to_complete_task'
CLEANUP_OLD_BATCHES_TASK = 'cleanup_old_batches_task'
SECONDS_IN_HOUR = 60 * 60
SECONDS_IN_MINUTE = 60
EIS_DOMAIN_NAME = 'expand_inventory_selection'


def build_medivac_s3_destination_uri(
    task_instance: TaskInstance, execution_date: DateTime, storage_bucket: str, domain_name: str, dataset_name: str,
    is_prod_environment: bool, **kwargs
):
    timestamp = execution_date.strftime("%H%M%S%f")
    date = execution_date.strftime("%Y%m%d")
    storage_prefix = (
        f'env=prod/{domain_name}/{dataset_name}/date={date}/timestamp={timestamp}'
        if is_prod_environment else f'env=test/{domain_name}/{dataset_name}/date={date}/timestamp={timestamp}'
    )
    s3_destination_uri = f's3://{storage_bucket}/{storage_prefix}'
    task_instance.xcom_push(S3_DEST_URI_BUCKET_KEY, storage_bucket)
    task_instance.xcom_push(S3_DEST_URI_PREFIX_KEY, storage_prefix)
    task_instance.xcom_push(S3_DEST_URI_KEY, s3_destination_uri)


def build_s3_destination_uri_task(storage_bucket: str, domain_name: str, dataset_name: str, is_prod_environment: bool) -> PythonOperator:
    return PythonOperator(
        task_id=BUILD_S3_DESTINATION_URI_TASK_ID,
        python_callable=build_medivac_s3_destination_uri,
        provide_context=True,
        op_kwargs={
            'execution_date': now('UTC'),
            'domain_name': domain_name,
            'storage_bucket': storage_bucket,
            'dataset_name': dataset_name,
            'is_prod_environment': is_prod_environment
        }
    )


def create_version(task_instance: TaskInstance, inventory_batch_type_id: int, **kwargs):
    sql_hook = MsSqlHook(mssql_conn_id=PROVISIONING_SQL_CONNECTION, schema=PROVISIONING_DB_NAME)
    connection = sql_hook.get_conn()
    sql_hook.set_autocommit(connection, True)
    cursor = connection.cursor()
    cursor.execute(
        f"""
        DECLARE @MyNewVersionId INT;
        EXEC dbo.prc_CreateNewInventoryBatchVersion @InventoryBatchTypeId = {inventory_batch_type_id}, @NewVersionId = @MyNewVersionId OUTPUT;
        SELECT @MyNewVersionId as NewVersionId;
    """
    )
    new_version_id = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    print(f'New version is: {new_version_id}')
    task_instance.xcom_push(key=VERSION_ID_KEY, value=new_version_id)


def build_create_version_task(inventory_batch_type_id: int) -> PythonOperator:
    return PythonOperator(
        task_id=CREATE_VERSION_TASK_ID,
        python_callable=create_version,
        provide_context=True,
        op_kwargs={'inventory_batch_type_id': inventory_batch_type_id}
    )


def set_version_to_complete(task_instance: TaskInstance, inventory_batch_type_id: int, **kwargs):
    version_id = task_instance.xcom_pull(task_ids=CREATE_VERSION_TASK_ID, key=VERSION_ID_KEY)
    print(f'setting version {version_id} to complete')
    sql_hook = MsSqlHook(mssql_conn_id=PROVISIONING_SQL_CONNECTION, schema=PROVISIONING_DB_NAME)
    connection = sql_hook.get_conn()
    sql_hook.set_autocommit(connection, True)
    cursor = connection.cursor()
    cursor.execute(
        f'EXEC dbo.prc_SetInventoryBatchVersionComplete @InventoryBatchTypeId = {inventory_batch_type_id}, @VersionId = {version_id}'
    )
    cursor.close()
    connection.close()


def build_set_version_to_complete_task(inventory_batch_type_id: int) -> PythonOperator:
    return PythonOperator(
        task_id=SET_VERSION_TO_COMPLETE_TASK,
        python_callable=set_version_to_complete,
        provide_context=True,
        op_kwargs={'inventory_batch_type_id': inventory_batch_type_id}
    )


def format_seconds_to_hhmmss(seconds: int) -> str:
    hours, remaining_minutes = divmod(seconds, SECONDS_IN_HOUR)
    minutes, remaining_seconds = divmod(remaining_minutes, SECONDS_IN_MINUTE)
    return f"{hours:02}:{minutes:02}:{remaining_seconds:02}"


def cleanup_old_batches(sproc_name: str, batch_size: int, wait_time_between_batches_in_seconds: int):
    formatted_wait_time = format_seconds_to_hhmmss(wait_time_between_batches_in_seconds)
    sql_hook = MsSqlHook(mssql_conn_id=PROVISIONING_SQL_CONNECTION, schema=PROVISIONING_DB_NAME)
    connection = sql_hook.get_conn()
    sql_hook.set_autocommit(connection, True)
    cursor = connection.cursor()
    execute_command = f"EXEC dbo.{sproc_name} @BatchSize = {batch_size}, @WaitTime = '{formatted_wait_time}'"
    print(f"Executing: {execute_command} ")
    cursor.execute(execute_command)
    cursor.close()
    connection.close()
    print("SPROC is executed successfully")


def build_cleanup_old_batches_task(cleanup_sproc_name: str, batch_size: int, wait_time_between_batches_in_seconds: int) -> PythonOperator:
    return PythonOperator(
        task_id=CLEANUP_OLD_BATCHES_TASK,
        python_callable=cleanup_old_batches,
        op_kwargs={
            'sproc_name': cleanup_sproc_name,
            'batch_size': batch_size,
            'wait_time_between_batches_in_seconds': wait_time_between_batches_in_seconds
        }
    )
