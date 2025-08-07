import logging

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ttd.ttdslack import dag_post_to_slack_callback

FETCHED_DATA_FROM_SQL_SERVER_TASK_ID = 'fetch_data_from_sql_server'

dag_name = "Sync_RecipientGdprVendorId_From_Provdb_to_Snowflake"
provisioning_conn_id = 'provdb-readonly'
execution_interval = timedelta(days=1)

alarm_slack_datasrvc_channel = "#scrum-data-services-alarms"

snowflake_conn_id_default = "snowflake"
snowflake_warehouse_default = "TTD_AIRFLOW"
snowflake_database_default = "THETRADEDESK"
snowflake_reds_schema_default = "REDS"


def ensure_table_exists():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS RecipientGdprVendorId (
        RecipientId INTEGER NOT NULL,
        ProviderId INTEGER NOT NULL,
        VendorId INTEGER NOT NULL
    );
    """
    snowflake_hook = get_snowflake_hook()
    snowflake_hook.run(create_table_sql)


def fetch_data_from_sql_server(**context):
    mssql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    sql = "SELECT RecipientId, ProviderId, VendorId FROM dbo.RecipientGdprVendorId"
    data = mssql_hook.get_pandas_df(sql).to_dict(orient='records')

    return data


def insert_data_to_snowflake(**context):
    data = context['ti'].xcom_pull(task_ids=FETCHED_DATA_FROM_SQL_SERVER_TASK_ID)

    if not data:
        logging.info("No data found in XCom from fetch_data_task")
        return

    snowflake_hook = get_snowflake_hook()
    snowflake_conn = snowflake_hook.get_conn()
    cursor = snowflake_conn.cursor()

    try:
        cursor.execute("BEGIN TRANSACTION")

        cursor.execute("TRUNCATE TABLE RecipientGdprVendorId")

        insert_query = f"""
        INSERT INTO RecipientGdprVendorId (RecipientId, ProviderId, VendorId)
        VALUES {', '.join([f"({row['RecipientId']}, {row['ProviderId']}, {row['VendorId']})" for row in data])};
        """
        cursor.execute(insert_query)

        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()


def get_snowflake_hook():
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id_default,
        warehouse=snowflake_warehouse_default,
        database=snowflake_database_default,
        schema=snowflake_reds_schema_default
    )
    return snowflake_hook


default_args = {
    "owner": "datasrvc",
    'start_date': datetime(2024, 11, 18),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

with DAG(dag_id=dag_name, default_args=default_args, schedule_interval=execution_interval, tags=["DATASRVC", "REDS", "DSR"],
         on_failure_callback=dag_post_to_slack_callback(
             dag_name=dag_name,
             step_name="parent dagrun",
             slack_channel=alarm_slack_datasrvc_channel,
         ), max_active_runs=1, start_date=datetime(2025, 6, 1)) as dag:
    ensure_table_exists_task = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=ensure_table_exists,
    )

    fetch_data_from_sql_server_task = PythonOperator(
        task_id=FETCHED_DATA_FROM_SQL_SERVER_TASK_ID,
        python_callable=fetch_data_from_sql_server,
    )

    insert_data_to_snowflake_task = PythonOperator(
        task_id='insert_data_to_snowflake',
        python_callable=insert_data_to_snowflake,
        provide_context=True,  # Required for XCom access
    )

    ensure_table_exists_task >> fetch_data_from_sql_server_task >> insert_data_to_snowflake_task
