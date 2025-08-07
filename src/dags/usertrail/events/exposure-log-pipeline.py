from datetime import datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import (
    S3PythonDatabricksTask,
)
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from dags.usertrail.datasets import user_trail_exposures_dataset

dag_name = "usertrail-exposure-pipeline"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/user-trail/release/3151465/latest/user_trail_pipelines-0.0.1-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/user-trail/release/3151465/latest/code/user_trail_pipelines/exposure_job.py"

run_date = '{{ data_interval_start.strftime("%Y-%m-%d") }}'
run_date_hourly = '{{ data_interval_start.strftime("%Y-%m-%d %H:00:00") }}'
version = "1"
s3_prefix = "s3://ttd-usertrail-data"
hourly = False
isVirtual = True

# Arguments
arguments = [
    "--logLevel",
    "Info",
    "--env",
    env.dataset_write_env,
    "--run_date",
    run_date,
    "--run_date_hourly",
    run_date_hourly,
    "--s3_prefix",
    s3_prefix,
    "--version",
    version,
    "--hourly",
    hourly,
    "--isVirtual",
    isVirtual,
]

###############################################################################
# DAG Definition
###############################################################################

# The top-level dag
usertrail_exposure_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 5, 6),
    schedule_interval="0 4 * * *",
    dag_tsg="https://thetradedesk.atlassian.net/l/cp/mCvqNeUz",
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=15),
    slack_channel="#scrum-user-trail",
    slack_alert_only_for_prod=True,
    tags=["USERTRAIL"],
)

dag = usertrail_exposure_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

events_log_collector_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="usertrail_exposure_data_available",
        datasets=[user_trail_exposures_dataset],
        ds_date='{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 6,
    )
)

###############################################################################
# Databricks Workflow
###############################################################################

parquet_delta_task = DatabricksWorkflow(
    job_name="user_trail_exposures_job",
    cluster_name="user_trail_exposures_process_cluster",
    cluster_tags={
        "Team": "USERTRAIL",
        "Project": "UT Exposure",
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
            job_name="user_trail_exposure",
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
(usertrail_exposure_dag >> events_log_collector_sensor >> parquet_delta_task >> final_dag_status_step)
