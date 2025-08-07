"""DAG for bidfeedback/click incremental load."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ttd.python_versions import PythonVersions
from ttd.spark import (
    EbsConfiguration,
    SparkVersionSpec,
    SparkWorkflow,
    CustomBackends,
    Databricks,
)
from ttd.eldorado.databricks.databricks_runtime import DatabricksRuntimeVersion
from ttd.spark_workflow.tasks.pyspark_task import PySparkTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
import logging

env = TtdEnvFactory.get_from_system().execution_env
custom_solution_instances = ["CSOL-12422"]
parsed_instances = ",".join(custom_solution_instances)
dag_name = "bidfeedback_click_incremental_dbr_load"
worker_instance_count = 8
spark_config = [("conf", "spark.sql.shuffle.partitions=auto")]
code_base_path = "s3://ttd-build-artefacts/csol/csol-12422-reach-overlap/"
code_sub_dir = "release/latest" if env == "prod" else "mergerequests/zhy-CSOL-13233-ftr-dag-test/latest"
python_wheel_name = "campaign_reach_overlap-1.0.0-py3-none-any.whl"
python_wheel_path = f"{code_base_path}{code_sub_dir}/{python_wheel_name}"
bidfeedback_click_entry_point = f"{code_base_path}{code_sub_dir}/code/bid_feedback_click_inc_load.py"

# The top-level dag
bidfeedback_click_incremental_load_dbr_dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 7, 22),
    schedule_interval="0 11 * * *" if env == "prod" else None,
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel=f"#csol-12422-campaign-reach-overlap-notification-{'prod' if env == 'prod' else 'qa'}",
    slack_alert_only_for_prod=False,
    tags=["CSOL"],
)
adag: DAG = bidfeedback_click_incremental_load_dbr_dag.airflow_dag


def push_dates_to_xcom(**context):
    """Function to get dates from DAG config and push to xcom."""
    dag_run = context.get("dag_run")
    start_date = dag_run.conf.get("start_date") if dag_run and dag_run.conf else None
    end_date = dag_run.conf.get("end_date") if dag_run and dag_run.conf else None
    override = dag_run.conf.get("override") if dag_run and dag_run.conf else None

    start_date_str = (datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d") if start_date else "")
    end_date_str = (datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d") if end_date else "")

    # Push to XCom
    context['ti'].xcom_push(key='start_date', value=start_date_str)
    context['ti'].xcom_push(key='end_date', value=end_date_str)
    context['ti'].xcom_push(key='override', value=override)

    logging.info(f"Pushed start_date={start_date}, end_date={end_date}, override={override} to XCom")
    return start_date_str, end_date_str


push_dates_to_xcom_task = OpTask(
    op=PythonOperator(dag=adag, task_id="push_dates_to_xcom_task", python_callable=push_dates_to_xcom, provide_context=True)
)

# Spark task for incremental bidfeedback/click load.
task_start = PySparkTask(
    task_name="bidfeedback-incremental-load",
    python_entrypoint_location=bidfeedback_click_entry_point,
    additional_command_line_arguments=[
        "--env",
        env,
        "--instance-ids",
        parsed_instances,
        "--start-date",
        "{{ ti.xcom_pull(task_ids='push_dates_to_xcom_task', key='start_date') }}",
        "--end-date",
        "{{ ti.xcom_pull(task_ids='push_dates_to_xcom_task', key='end_date') }}",
        "--override",
        "{{ ti.xcom_pull(task_ids='push_dates_to_xcom_task', key='override') }}",
    ],
)

py_spark_task = SparkWorkflow(
    job_name="bidfeedback-incremental-load",
    instance_type=I3.i3_2xlarge(),
    driver_instance_type=I3.i3_4xlarge(),
    instance_count=worker_instance_count,
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_11.value,
    whls_to_install=[python_wheel_path],
    ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
    tasks=[task_start],
    retries=0,
    backend_chooser=CustomBackends(runtimes=[Databricks()]),
    candidate_databricks_runtimes=[DatabricksRuntimeVersion.DB_15_4.value],
    tags={
        "Process": "Elililly-bidfeedback-incremental-load",
        "Team": "CSOL"
    },
)

# Set task dependencies
bidfeedback_click_incremental_load_dbr_dag >> push_dates_to_xcom_task >> py_spark_task
