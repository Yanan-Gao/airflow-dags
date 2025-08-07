from datetime import datetime, timedelta
from typing import List, Tuple

import logging
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator

logger = logging.getLogger(__name__)

dag_name = "budget-delta-maintenance-databricks"

# Environment
env = TtdEnvFactory.get_from_system()
retention_hours = 24 * 2

WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/databricks_maintenance_job.py"

base_path = f"s3://ttd-budget-calculation-lake/env={env.dataset_write_env}"

logtypes = [
    'budgetadgroupcalculationresult', 'budgetcampaigncalculationresult', 'budgetrvaluecalculationresult',
    'budgetvirtualcampaigncalculationresult', 'budgetvirtualadgroupcalculationresult', 'budgetvolumecontrolcalculationresult',
    'budgetspendrateoptimizercalculationresult', 'budgetspendrateoptimizerdatacenterresult'
]

metrics = ['hard_min_throttle_delta', 'throttle_metric_campaign_delta', 'throttle_metric_delta']


def get_delta_tables() -> List[Tuple[str, str]]:
    tables = []
    for logtype in logtypes:
        table_path = f"{base_path}/CalculationResultTables/{logtype}"
        tables.append((logtype, table_path))
    for metric in metrics:
        table_path = f"{base_path}/virtual-aggregate-pacing-hourly-statistics/v=2/metric={metric}"
        tables.append((metric, table_path))
    return tables


###############################################################################
# DAG
###############################################################################

# The top-level dag
maintenance_delta_databricks_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 1, 24),
    schedule_interval='0 0 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FoALC',
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = maintenance_delta_databricks_dag.airflow_dag

###############################################################################
# Task Generation
###############################################################################
delta_tables = get_delta_tables()
tasks = []

for table, path in delta_tables:
    logger.info(f'Creating maintenance tasks for table: {table}')

    vacuum_sql = f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS;"

    optimize_sql = f"OPTIMIZE  delta.`{path}`;"

    vacuum_task = S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=["--query", vacuum_sql],
        job_name=f"vacuum_{table}",
        whl_paths=[WHL_PATH],
    )

    optimize_task = S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=["--query", optimize_sql],
        job_name=f"optimize_{table}",
        whl_paths=[WHL_PATH],
    )

    vacuum_task >> optimize_task
    tasks.append(vacuum_task)
    tasks.append(optimize_task)

###############################################################################
# Databricks Workflow
###############################################################################

databricks_housekeeping_task = DatabricksWorkflow(
    job_name="budget_databricks_housekeeping_job",
    cluster_name="budget_databricks_housekeeping_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget Databricks Housekeeping",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=4,
        ebs_volume_size_gb=256,
    ),
    worker_node_type="r7g.4xlarge",
    driver_node_type="m7g.4xlarge",
    worker_node_count=20,
    use_photon=True,
    tasks=tasks,
    databricks_spark_version="16.2.x-scala2.12",
    spark_configs={
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "3",
        "spark.executor.cores": "4",
        "spark.shuffle.service.enabled": "true",
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
maintenance_delta_databricks_dag >> databricks_housekeeping_task >> final_dag_status_step
