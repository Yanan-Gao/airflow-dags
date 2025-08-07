from datetime import datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from dags.datperf.datasets import rtb_bidrequest_v5, rtb_bidfeedback_v5
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow

dag_name = "budget-cpm-metrics"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/cpm_metrics_job.py"

run_date = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
run_datetime = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M') }}"
version = '1'
s3_prefix = 's3://ttd-budget-calculation-lake'
s3_env_suffix = 'budget-cpm-metrics'

# Arguments
arguments = [
    "--logLevel",
    "Info",
    "--env",
    env.dataset_write_env,
    "--run_datetime",
    run_datetime,
    "--s3_prefix",
    s3_prefix,
    "--s3_env_suffix",
    s3_env_suffix,
    "--version",
    version,
]

###############################################################################
# DAG Definition
###############################################################################

# The top-level dag
cpm_metrics_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 3, 16),
    schedule_interval='0 * * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FoALC',
    retries=3,
    max_active_runs=10,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = cpm_metrics_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

# Budget data
vertica_budget_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_data_available',
        datasets=[
            rtb_bidfeedback_v5.with_check_type("hour"),
            rtb_bidrequest_v5.with_check_type("hour"),
        ],
        ds_date="{{ data_interval_end.strftime(\"%Y-%m-%d %H:00:00\") }}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 10,
    )
)

###############################################################################
# Databricks Workflow
###############################################################################

parquet_delta_task = DatabricksWorkflow(
    job_name="budget_cpm_metrics_computation_job",
    cluster_name="budget_cpm_metrics_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget CPM Metrics",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r8g.2xlarge",
    driver_node_type="m8g.2xlarge",
    worker_node_count=5,
    use_photon=True,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=arguments,
            job_name="budget_cpm_metrics",
            whl_paths=[WHL_PATH],
        ),
    ],
    databricks_spark_version="16.4.x-scala2.13",
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
cpm_metrics_dag >> vertica_budget_sensor >> parquet_delta_task >> final_dag_status_step
