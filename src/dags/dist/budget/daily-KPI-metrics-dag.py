from datetime import datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState

dag_name = "budget-daily-kpi-metrics"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/kpi_metrics_job.py"

run_datetime = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M') }}"
version = '1'
s3_prefix = 's3://ttd-budget-calculation-lake'
s3_env_suffix = 'kpi-metrics'
isDailyKpiMetrics = True

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
    "--isDailyKpiMetrics",
    isDailyKpiMetrics,
]

###############################################################################
# DAG Definition
###############################################################################

# The top-level dag
daily_kpi_metrics_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 3, 15),
    schedule_interval='0 6 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FoALC',
    retries=3,
    max_active_runs=10,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = daily_kpi_metrics_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

janus_kpi_revenue_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="janus_kpi_revenue_sensor",
        external_dag_id="janus-kpis-rollup-revenue-distributed-algo-adv-cmp-ag-daily-agg",
        external_task_id=None,
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        execution_date_fn=lambda dt: (dt).replace(hour=0, minute=0, second=0, microsecond=0),
        mode="reschedule",
        poke_interval=600,
        timeout=60 * 60 * 10,
        dag=dag
    )
)

janus_kpi_conversion_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="janus_kpi_conversion_sensor",
        external_dag_id="janus-kpis-rollup-conversion-distributed-algo-adv-cmp-ag-daily-agg",
        external_task_id=None,
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        execution_date_fn=lambda dt: (dt).replace(hour=0, minute=0, second=0, microsecond=0),
        mode="reschedule",
        poke_interval=600,
        timeout=60 * 60 * 10,
        dag=dag
    )
)
###############################################################################
# Databricks Workflow
###############################################################################

kpi_task = DatabricksWorkflow(
    job_name="budget_daily_kpi_job",
    cluster_name="budget_daily_kpi_metrics_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget KPI Metrics",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r8g.xlarge",
    driver_node_type="m8g.xlarge",
    worker_node_count=1,
    use_photon=True,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=arguments,
            job_name="budget_daily_kpi_metrics",
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
daily_kpi_metrics_dag >> janus_kpi_revenue_sensor >> janus_kpi_conversion_sensor >> kpi_task >> final_dag_status_step
