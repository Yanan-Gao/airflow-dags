from datetime import datetime, timedelta

from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.datperf.datasets import virtual_adgroup_calculation_result_dataset, r_value_calculation_result_dataset, \
    volume_control_calculation_result_dataset

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState

dag_name = "budget-virtual-hourly-cpm-controller-metrics"

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/cpm_controller_metrics_job.py"

# Environment
env = TtdEnvFactory.get_from_system()

run_date_hourly = "{{ (data_interval_start + macros.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M') }}"
version = '2'
s3_prefix = 's3://ttd-budget-calculation-lake'
s3_env_suffix = 'virtual-aggregate-pacing-hourly-statistics'
daily_gauntlet_allowed_count_threshold = 1000
is_virtual = "True"
is_hourly = "True"

###############################################################################
# DAG
###############################################################################

# The top-level dag
cpm_controller_metrics_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 6, 10),
    schedule_interval='0 1-22 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/l/cp/aK79cXPo',
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = cpm_controller_metrics_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

###############################################################################
# S3 dataset sensors
###############################################################################

# Budget data
vertica_budget_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_data_available',
        datasets=[
            virtual_adgroup_calculation_result_dataset.with_check_type("hour"),
            r_value_calculation_result_dataset.with_check_type("hour"),
            volume_control_calculation_result_dataset.with_check_type("hour"),
        ],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6
    )
)

# CPM metric data
budget_cpm_metrics_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="cpm_metrics_sensor",
        external_dag_id="budget-cpm-metrics",
        external_task_id=None,
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        execution_date_fn=lambda dt: (dt).replace(minute=0, second=0, microsecond=0),
        mode="reschedule",
        poke_interval=600,
        timeout=60 * 60 * 12,
        dag=dag
    )
)

###############################################################################
# steps
###############################################################################

cpm_controller_metrics_task = DatabricksWorkflow(
    job_name="virtual_hourly_cpm_controller_metrics_job",
    cluster_name="virtual_hourly_cpm_controller_metrics_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget CPM Controller Metrics",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r8g.xlarge",
    driver_node_type="m8g.xlarge",
    worker_node_count=2,
    use_photon=True,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=[
                "--logLevel", "Info", "--env", env.dataset_write_env, "--run_date", run_date_hourly, "--s3_prefix", s3_prefix,
                "--s3_env_suffix", s3_env_suffix, "--daily_gauntlet_allowed_count_threshold",
                str(daily_gauntlet_allowed_count_threshold), "--version", version, "--is_virtual", is_virtual, "--is_hourly", is_hourly
            ],
            job_name="virtual_hourly_cpm_controller_metrics",
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

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG dependencies
cpm_controller_metrics_dag >> budget_cpm_metrics_sensor >> vertica_budget_sensor >> cpm_controller_metrics_task >> final_dag_status_step
