from datetime import datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from dags.datperf.datasets import platformreport_dataset, \
    adgroup_dataset, campaign_dataset, advertiser_dataset, currencyexchangerate_dataset
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState

dag_name = "budget-throttle-metrics"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/throttle_metrics_job.py"

run_date = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
version = '2'
gauntlet_threshold = 20
keep_rate_threshold = 0.8
s3_prefix_parquet = 's3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control'
s3_env_suffix = 'aggregate-pacing-statistics'
s3_prefix_delta = 's3://ttd-budget-calculation-lake'
write_delta_liquid_clustering = True
write_parquet = True

# Arguments
arguments = [
    "--logLevel", "Info", "--env", env.dataset_write_env, "--run_date", run_date, "--s3_prefix_parquet", s3_prefix_parquet,
    "--s3_prefix_delta", s3_prefix_delta, "--s3_env_suffix", s3_env_suffix, "--gauntlet_block_spend_threshold",
    str(gauntlet_threshold), "--keep_rate_threshold",
    str(keep_rate_threshold), "--version", version, "--write_delta_with_liquid_clustering",
    str(write_delta_liquid_clustering), "--write_parquet",
    str(write_parquet)
]

###############################################################################
# DAG Definition
###############################################################################

# The top-level dag
throttle_metrics_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 3 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/9AqVGg',
    retries=3,
    max_active_runs=10,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-alarms-high-pri",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = throttle_metrics_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

# Budget data
vertica_budget_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_data_available',
        datasets=[
            platformreport_dataset.with_check_type("day"),
        ],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 24 hours
        timeout=60 * 60 * 24
    )
)

budget_data_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="budget-calculation-results-sensor",
        external_dag_id="budget-calculation-results-delta",
        external_task_id=None,
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        execution_date_fn=lambda dt: (dt).replace(hour=23, minute=0, second=0, microsecond=0),
        mode="reschedule",
        poke_interval=600,
        timeout=60 * 60 * 10,
        dag=dag
    )
)

cpm_proximity_data_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="budget-cpm-controller-metrics-dag",
        external_dag_id="budget-cpm-controller-metrics",
        external_task_id=None,
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        execution_date_fn=lambda dt: (dt).replace(minute=0, second=0, microsecond=0),
        mode="reschedule",
        poke_interval=600,
        timeout=60 * 60 * 10,
        dag=dag
    )
)

provisioning_budget_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="budget_provisioning_data_available",
        datasets=[adgroup_dataset, campaign_dataset, advertiser_dataset, currencyexchangerate_dataset],
        ds_date="{{ data_interval_end.strftime(\"%Y-%m-%d 00:00:00\") }}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 24
    )
)

###############################################################################
# Databricks Workflow
###############################################################################

parquet_delta_task = DatabricksWorkflow(
    job_name="budget_throttle_metrics_job",
    cluster_name="budget_throttle_metrics_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget Throttle Metrics",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=256,
    ),
    worker_node_type="r7g.4xlarge",
    driver_node_type="m7g.4xlarge",
    worker_node_count=10,
    use_photon=True,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=arguments,
            job_name="budget_throttle_metrics",
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
throttle_metrics_dag >> vertica_budget_sensor >> provisioning_budget_sensor >> budget_data_sensor >> cpm_proximity_data_sensor >> parquet_delta_task >> final_dag_status_step
