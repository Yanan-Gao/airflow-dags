from datetime import datetime, timedelta

from dags.datperf.datasets import campaign_throttlemetric_dataset
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

import logging

logger = logging.getLogger(__name__)

dag_name = "auto_end_budget_split_experiments_dag"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/auto_end_experiment_job.py"

run_date = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"

###############################################################################
# DAG Definition
###############################################################################
# The top-level dag
# Runs every day at 8am and 4pm
auto_end_budget_split_experiment_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 3, 27),
    schedule_interval='0 8,16 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FoALC',
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-split-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = auto_end_budget_split_experiment_dag.airflow_dag

###############################################################################
# Data Sensors
###############################################################################
throttle_metrics_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_data_available',
        datasets=[
            campaign_throttlemetric_dataset,
        ],
        ds_date="{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d 23:00:00') }}",
        poke_interval=60 * 10,
        # wait up to 24 hours
        timeout=60 * 60 * 24
    )
)

###############################################################################
# Databricks Workflow
###############################################################################
environment = "prod" if env == TtdEnvFactory.prod else "prodTest"

end_budget_split_experiment_dag = DatabricksWorkflow(
    job_name="budget_auto_end_budget_split_experiments_job",
    cluster_name="budget_auto_end_budget_split_experiments_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget Split Experiment",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r8g.4xlarge",
    driver_node_type="m8g.4xlarge",
    worker_node_count=1,
    use_photon=True,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=["--env", environment, "--run_date", run_date],
            job_name="auto_end_experiment_job",
            whl_paths=[WHL_PATH]
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
auto_end_budget_split_experiment_dag >> throttle_metrics_sensor >> end_budget_split_experiment_dag >> final_dag_status_step
