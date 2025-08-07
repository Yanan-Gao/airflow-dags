import logging
from datetime import datetime, timedelta

from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from dags.datperf.datasets import (
    volume_control_calculation_result_dataset, r_value_calculation_result_dataset, adgroup_calculation_result_dataset,
    campaign_calculation_result_dataset, virtual_adgroup_calculation_result_dataset, virtual_campaign_calculation_result_dataset
)

logger = logging.getLogger(__name__)

dag_name = "budget-calculation-results-delta"

# Environment Variables
env = TtdEnvFactory.get_from_system()
run_datetime = "{{ (data_interval_start).strftime('%Y-%m-%dT%H:%M') }}"
s3_base_path = f's3://ttd-budget-calculation-lake/env={env.dataset_write_env}/CalculationResultTables'
cluster_by = "CalculationTime,Partition,CampaignId"
cluster_by_adgroup = cluster_by + ",AdGroupId"

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/parquet_delta_job.py"
ENTRY_PATH_SQL = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/execute_sql_store_result_job.py"

# Arguments
arguments = ["--logLevel", "Info", "--run_datetime", run_datetime, "--base_path", s3_base_path]

###############################################################################
# DAG Definition
###############################################################################

# Define the top-level DAG
calculation_results_delta_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 1, 15),
    schedule_interval='0 * * * *',
    dag_tsg='https://thetradedesk.atlassian.net/l/cp/aK79cXPo',
    retries=3,
    max_active_runs=3,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = calculation_results_delta_dag.airflow_dag

###############################################################################
# S3 Dataset Sources
###############################################################################

# Define the sensor task for dataset availability
vertica_budget_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_data_available',
        datasets=[
            volume_control_calculation_result_dataset.with_check_type("hour"),
            r_value_calculation_result_dataset.with_check_type("hour"),
            campaign_calculation_result_dataset.with_check_type("hour"),
            adgroup_calculation_result_dataset.with_check_type("hour"),
            virtual_adgroup_calculation_result_dataset.with_check_type("hour"),
            virtual_campaign_calculation_result_dataset.with_check_type("hour"),
        ],
        ds_date="{{ data_interval_end.strftime(\"%Y-%m-%d %H:00:00\") }}",
        poke_interval=60 * 2,  # Check every 2 minutes
        timeout=60 * 60 * 10,  # Wait up to 10 hours
    )
)

###############################################################################
# Databricks Workflow
###############################################################################

calculation_results_tasks = [
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetadgroupcalculationresult", "--cluster_by", cluster_by_adgroup],
        job_name="ad_group_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetcampaigncalculationresult", "--cluster_by", cluster_by],
        job_name="campaign_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetvirtualadgroupcalculationresult", "--cluster_by", cluster_by_adgroup],
        job_name="virtual_ad_group_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetvirtualcampaigncalculationresult", "--cluster_by", cluster_by],
        job_name="virtual_campaign_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetvolumecontrolcalculationresult", "--cluster_by", cluster_by_adgroup],
        job_name="volume_control_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetrvaluecalculationresult", "--cluster_by", cluster_by_adgroup],
        job_name="r_value_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetspendrateoptimizercalculationresult", "--cluster_by", cluster_by_adgroup],
        job_name="spend_rate_optimizer_calculation_delta_task",
        whl_paths=[WHL_PATH],
    ),
    S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH,
        args=arguments + ["--logtype", "budgetspendrateoptimizerdatacenterresult", "--cluster_by", cluster_by_adgroup],
        job_name="spend_rate_optimizer_datacenter_delta_task",
        whl_paths=[WHL_PATH],
    ),
]

# Format today's date as YYYY/MM/DD
today = "{{ (data_interval_start).strftime('%Y/%m/%d/%H') }}"

for logtype, use_variant_key in [('budgetsplitexperimentinfo', False), ('budgetsplitexperimentarminfo', True)]:
    # Delta table path for storing experiment data
    table_path = f"s3://ttd-budget-calculation-lake/env={env.dataset_write_env}/experiments/{logtype}"

    # Source parquet files location containing the current datetime's logs
    parquet_files = f"s3://thetradedesk-useast-logs-2/{logtype}/collected/{today}"

    key_columns = [('ExperimentId', 'STRING')]
    if use_variant_key:
        key_columns.append(('JanusVariantKey', 'STRING'))
    columns = '\n'.join([f'{key} {data_type},' for key, data_type in key_columns])

    sql_create = f"""
    -- Create Delta table if not exists with required schema
    CREATE TABLE IF NOT EXISTS delta.`{table_path}`
    (
      LogEntryTime TIMESTAMP,
      {columns}
      _delta_update_time TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (ExperimentId, LogEntryTime);
    """

    match_condition = ' AND '.join([f'source.{key} = target.{key}' for key, _ in key_columns])
    column_names = ', '.join([column for column, _ in key_columns])
    sql_merge = f"""
    -- Merge new experiment data with schema evolution enabled
    MERGE WITH SCHEMA EVOLUTION INTO delta.`{table_path}` as target
    USING (
      -- Get latest version of each experiment by selecting max LogEntryTime
      SELECT
        source.* EXCEPT (row_num),
        current_timestamp() as _delta_update_time
      FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY {column_names} ORDER BY LogEntryTime DESC) as row_num
        FROM read_files(
            '{parquet_files}/',
            format => 'parquet',
            mergeSchema => 'true'
        )
        WHERE IsLeader
      ) source
      WHERE row_num = 1
    ) as source
    ON {match_condition}

    -- Update existing experiments only if source has newer version
    WHEN MATCHED AND source.LogEntryTime > target.LogEntryTime
    THEN UPDATE SET *

    -- Insert new experiments not yet in target
    WHEN NOT MATCHED
    THEN INSERT *;
    """

    create_task = S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH_SQL,
        args=["--query", sql_create],
        job_name=f"{logtype}_create_delta_task",
        whl_paths=[WHL_PATH],
    )
    merge_task = S3PythonDatabricksTask(
        entrypoint_path=ENTRY_PATH_SQL,
        args=["--query", sql_merge, "--directory-to-be-present", parquet_files],
        job_name=f"{logtype}_merge_delta_task",
        whl_paths=[WHL_PATH],
    )

    create_task >> merge_task

    calculation_results_tasks.append(create_task)
    calculation_results_tasks.append(merge_task)

parquet_delta_task = DatabricksWorkflow(
    job_name="budget_calculation_results_job",
    cluster_name="budget_calculation_results_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget Hourly VirtualThrottle Metrics",
        "Environment": env.dataset_write_env,
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r8g.4xlarge",
    driver_node_type="m8g.4xlarge",
    worker_node_count=10,
    use_photon=True,
    tasks=calculation_results_tasks,
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
calculation_results_delta_dag >> vertica_budget_sensor >> parquet_delta_task >> final_dag_status_step
