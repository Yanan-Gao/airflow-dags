from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import io
import logging
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from dags.usertrail.datasets import user_trail_events_dataset
from dags.usertrail.events.custom_events.daily_sql import get_requested_reports

dag_name = "usertrail-events-dedupe"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/user-trail/release/3275074/3275074/user_trail_pipelines-0.0.1-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/user-trail/release/3275074/latest/code/user_trail_pipelines/job.py"

run_date = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
run_date_hourly = "{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}"
version = '1'
s3_prefix = 's3://ttd-usertrail-data'
bucket = 'ttd-usertrail-data'
hourly = False
isVirtual = True

# Arguments
arguments = [
    "--logLevel", "Info", "--env", env.dataset_write_env, "--run_date", run_date, "--run_date_hourly", run_date_hourly, "--s3_prefix",
    s3_prefix, "--version", version, "--hourly", hourly, "--isVirtual", isVirtual
]

###############################################################################
# DAG Definition
###############################################################################

# The top-level dag
usertrail_dedup_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 3, 17),
    schedule_interval='0 3 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/l/cp/mCvqNeUz',
    retries=3,
    max_active_runs=10,
    retry_delay=timedelta(minutes=5),
    slack_channel="#scrum-user-trail",
    slack_alert_only_for_prod=True,
    tags=["USERTRAIL"],
)

dag = usertrail_dedup_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

events_log_collector_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="usertrail_events_data_available",
        datasets=[user_trail_events_dataset],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d %H:%M:%S\") }}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 6
    )
)

###############################################################################
# Python Task to Fetch additional activities
###############################################################################


def load_and_write_activities_to_s3(data_interval_start: datetime, **context):
    sql_connection = 'provisioning_replica'  # provdb-bi.adsrvr.org
    db_name = 'Provisioning'
    sql_hook = MsSqlHook(mssql_conn_id=sql_connection, schema=db_name)
    start_date = data_interval_start.strftime('%Y-%m-%d')
    query = get_requested_reports(start_date)
    logging.info(query)
    activities = sql_hook.get_pandas_df(query)

    buffer = io.BytesIO()
    activities.to_parquet(buffer, engine="pyarrow", index=False, coerce_timestamps="us")
    buffer.seek(0)

    s3 = AwsCloudStorage(conn_id='aws_default')
    s3.put_buffer(
        file_obj=buffer, bucket_name=bucket, key=f"env={env.dataset_write_env}/v=0/type=RequestReport/date={start_date}", replace=True
    )


activity_task = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=load_and_write_activities_to_s3,
        provide_context=True,
        task_id="load_and_write_activities_to_s3",
    )
)

###############################################################################
# Databricks Workflow
###############################################################################
parquet_delta_task = DatabricksWorkflow(
    job_name="user_trail_events_dedupe_job_2",
    cluster_name="user_trail_events_dedupe_cluster",
    cluster_tags={
        "Team": "USERTRAIL",
        "Project": "UT Events",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r7g.4xlarge",
    driver_node_type="m7g.4xlarge",
    worker_node_count=1,
    use_photon=True,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=arguments,
            job_name="user_trail_events_dedupe",
            whl_paths=[WHL_PATH],
        ),
    ],
    databricks_spark_version="15.4.x-scala2.12",
    spark_configs={
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
)
###############################################################################
# Final DAG Status Check
###############################################################################

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

###############################################################################
# DAG Dependencies
###############################################################################
usertrail_dedup_dag >> events_log_collector_sensor >> parquet_delta_task >> activity_task >> final_dag_status_step
