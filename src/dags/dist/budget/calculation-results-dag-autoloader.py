import logging
from datetime import datetime, timedelta
from typing import List

from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.autoscaling_config import DatabricksAutoscalingConfig
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.databricks.workflow import DatabricksWorkflow

logger = logging.getLogger(__name__)

# Environment Variables
env = TtdEnvFactory.get_from_system()
output_base_path = f's3://ttd-budget-calculation-lake/env={env.dataset_write_env}/autoloader-calculationresults-delta/'
cluster_by = "CalculationTime,MachineName,CampaignId"
cluster_by_adgroup = cluster_by + ",AdGroupId"

# https://thetradedesk.atlassian.net/wiki/spaces/EN/pages/218727541/Global+Otel+Endpoint
otel_endpoint = 'https://opentelemetry-global-gateway.gen.adsrvr.org/http/v1/metrics'

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/autoloader_delta_table_job.py"

# Full arguments list
arguments = [
    # Required parameters
    "--env",
    env.dataset_write_env,
    # Optional parameters with custom values
    "--batch_frequency",
    "30",
    # Retry configuration
    "--max_retries",
    "5",
    "--initial_backoff",
    "2.0",
    "--max_backoff",
    "120.0",
    "--backoff_factor",
    "2.0",
    "--jitter",
    "0.2",
    "--schema_hours_lookback",
    "3",
    "--timeout_minutes",
    str(6 * 60),  # 6 hours timeout
    # Additional logging parameter (if your app supports it)
    "--logLevel",
    "INFO",
    "--otel_endpoint",
    otel_endpoint
]


###############################################################################
# DAG Definition
###############################################################################
def get_calculation_result_dag(budget_log_type: str, driver_node_type: str = 'r8g.2xlarge', worker_node_type: str = 'r8g.2xlarge'):
    dag_name = f"{budget_log_type}_autoloader-delta"

    # Define the top-level DAG
    calculation_results_delta_dag = TtdDag(
        dag_id=dag_name,
        start_date=datetime(2025, 4, 3),
        schedule_interval='0 * * * *',
        dag_tsg='https://thetradedesk.atlassian.net/l/cp/aK79cXPo',
        retries=1,
        max_active_runs=1,
        run_only_latest=True,  # Prevents backfilling missed intervals
        retry_delay=timedelta(minutes=5),
        slack_channel="#taskforce-budget-metrics-alarms",
        slack_alert_only_for_prod=True,
        tags=["DIST"],
    )

    dag = calculation_results_delta_dag.airflow_dag

    ###############################################################################
    # Databricks Workflow
    ###############################################################################
    def arguments_log_type(log_type: str) -> List[str]:

        cluster_cols = cluster_by
        if 'adgroup' in log_type or log_type == 'budgetvolumecontrolcalculationresult' or log_type == 'budgetrvaluecalculationresult':
            cluster_cols = cluster_by_adgroup
        return arguments + [
            '--queue_url',
            f"https://sqs.us-east-1.amazonaws.com/003576902480/s3-{budget_log_type}-collected-{env.execution_env.lower()}-queue",
            '--parquet_files_path',
            f"s3://thetradedesk-useast-logs-2/{log_type}/collected",
            '--output_path',
            output_base_path + log_type,
            "--cluster_by",
            cluster_cols,
        ]

    calculation_results_tasks = [
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=arguments_log_type(budget_log_type),
            job_name=f"{budget_log_type}_calculation_delta_task",
            whl_paths=[WHL_PATH],
        ),
    ]

    parquet_delta_task = DatabricksWorkflow(
        job_name=f"{budget_log_type}_autoloader_calculation_results_job",
        cluster_name=f"{budget_log_type}_autoloader_calculation_results_cluster",
        cluster_tags={
            "Team": "DIST",
            "Project": f"{budget_log_type} autoloader to DeltaLake",
            "Environment": env.dataset_write_env,
        },
        ebs_config=DatabricksEbsConfiguration(
            ebs_volume_count=1,
            ebs_volume_size_gb=32,
        ),
        worker_node_type=worker_node_type,
        driver_node_type=driver_node_type,
        worker_node_count=1,
        use_photon=True,
        tasks=calculation_results_tasks,
        enable_elastic_disk=True,
        databricks_spark_version="16.4.x-scala2.13",
        spark_configs={
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
        },
        retries=3,
        retry_delay=timedelta(minutes=5),
        autoscaling_config=DatabricksAutoscalingConfig(min_workers=1, max_workers=10),
        bootstrap_script_actions=[
            ScriptBootstrapAction(
                path='s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/bootstrap/import-ttd-certs.sh',
                name='bootstrap-import-ttd-certs.sh'
            )
        ],
    )

    ###############################################################################
    # Final DAG Status Check
    ###############################################################################

    final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

    ###############################################################################
    # DAG Dependencies
    ###############################################################################
    calculation_results_delta_dag >> parquet_delta_task >> final_dag_status_step

    return dag


budgetvolumecontrolcalculationresult_dag = get_calculation_result_dag('budgetvolumecontrolcalculationresult')
budgetadgroupcalculationresult_dag = get_calculation_result_dag('budgetadgroupcalculationresult')
budgetrvaluecalculationresult_dag = get_calculation_result_dag('budgetrvaluecalculationresult', worker_node_type='r8g.large')
budgetcampaigncalculationresult_dag = get_calculation_result_dag('budgetcampaigncalculationresult', worker_node_type='r8g.large')
budgetvirtualadgroupcalculationresult_dag = get_calculation_result_dag(
    'budgetvirtualadgroupcalculationresult', driver_node_type='r8g.xlarge', worker_node_type='r8g.large'
)
budgetvirtualcampaigncalculationresult_dag = get_calculation_result_dag(
    'budgetvirtualcampaigncalculationresult', driver_node_type='r8g.xlarge', worker_node_type='r8g.large'
)
budgetspendrateoptimizercalculationresult_dag = get_calculation_result_dag(
    'budgetspendrateoptimizercalculationresult',
    driver_node_type='r8g.xlarge',
    worker_node_type='r8g.large',
)
budgetspendrateoptimizerdatacenterresult_dag = get_calculation_result_dag(
    'budgetspendrateoptimizerdatacenterresult',
    driver_node_type='r8g.xlarge',
    worker_node_type='r8g.large',
)
