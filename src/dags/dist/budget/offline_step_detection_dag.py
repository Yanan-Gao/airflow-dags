from datetime import datetime, timedelta

from airflow.sensors.s3_key_sensor import S3KeySensor

from dags.datperf.datasets import production_adgroup_budget_dataset

from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "offline-step-detection"

# Environment Variables
env = TtdEnvFactory.get_from_system()

# Python Wheel and Entry Point Paths
WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/mergerequests/<branch-name>/latest/budget_fiacre-latest-py3-none-any.whl"
ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/mergerequests/<branch-name>/latest/code/offline_step_detection_job.py"
if env.dataset_write_env == "prod":
    WHL_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/budget_fiacre-latest-py3-none-any.whl"
    ENTRY_PATH = "s3://ttd-build-artefacts/budget-fiacre/release/latest/code/offline_step_detection_job.py"

run_date = '{{ data_interval_start.strftime("%Y-%m-%d") }}'
run_date_compact = '{{ data_interval_start.strftime("%Y%m%d") }}'
s3_prefix = "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=test/ev_step_detection/v=1/"
# NB: we don't use "env=prod" here as this job has historically written simply
# to "dev".  Fixing this requires migrating all downstream consumers.
if env.dataset_write_env == "prod":
    s3_prefix = "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/ev_step_detection/v=1/"

# Arguments
arguments = [
    # unused; we always read from prod, and the write path is determined above (and not in pyspark)
    # "--env",
    # env.dataset_write_env,
    "--start-date",
    run_date,
    "--save-path",
    s3_prefix,
]

# This is the staging start date; adjust it to a recent date when testing this DAG to get fresh data
dag_start_date = datetime(2025, 7, 3)
if env.dataset_write_env == "prod":
    # This is the prod start date; if this gets updated, past run history will get lost; edit with caution
    dag_start_date = datetime(2025, 7, 17)

###############################################################################
# DAG Definition
###############################################################################

# The top-level dag
offline_step_detection_dag = TtdDag(
    dag_id=dag_name,
    start_date=dag_start_date,
    schedule_interval="30 7 * * *",
    dag_tsg="https://thetradedesk.atlassian.net/wiki/x/0wBAI",
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-alarms-low-pri",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = offline_step_detection_dag.airflow_dag

###############################################################################
# S3 dataset checks
###############################################################################

# The job reads from S3 snapshots of prov DB and ad group budgets, along with
# the rollout DA.  The algorithm is resilient to stale data from provisioning
# and simply looks for the latest full day of data.  That means we don't have a
# sensor for that here.  On the other hand, we do need up-to-date budgeting data
# so we force waits for that:
# * s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/value-algo-rollout/v=1/va_campaigns_date_changed
# * s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws

rollout_sensor_task = OpTask(
    op=S3KeySensor(
        task_id='value_da_rollout_available',
        poke_interval=60 * 10,  # 10m
        timeout=60 * 60 * 3,  # wait up to 3 hours before failing
        bucket_key=f'model_monitor/mission_control/dev/value-algo-rollout/v=1/va_campaigns_date_changed/date={run_date_compact}/_SUCCESS',
        bucket_name='thetradedesk-mlplatform-us-east-1',
        dag=dag
    )
)

# NB: we check for all 24 hours' data in the day in case they come out of order
# (so can't just check 23's data).  That is, the Vertica export for the budget
# snapshot does not have IsSerial set on the gating.
# https://gitlab.adsrvr.org/thetradedesk/teams/datasrvc/log-processing-config/-/blob/0f11a24eb3680dfbd7f08f4303bde1cd7f6417af/manifests/config/uncurated/VerticaExportToCloud.yaml#L813-827
ad_group_budget_snapshot_sensor_task = OpTask(
    op=DatasetCheckSensor(
        task_id='ad_group_budget_snapshot_available',
        datasets=[production_adgroup_budget_dataset],
        ds_date=f'{run_date} 00:00:00',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 3,  # wait up to 3 hours before failing
    )
)

###############################################################################
# Databricks Workflow
###############################################################################

databricks_task = DatabricksWorkflow(
    job_name="budget_offline_step_detection_job",
    cluster_name="budget_offline_step_detection_cluster",
    cluster_tags={
        "Team": "DIST",
        "Project": "Budget Offline step detection",
        "Environment": env.dataset_write_env,
    },
    worker_node_type="r7gd.16xlarge",
    worker_node_count=1,
    use_photon=False,
    enable_elastic_disk=False,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=ENTRY_PATH,
            args=arguments,
            job_name="budget_offline_step_detection",
            whl_paths=[WHL_PATH],
        ),
    ],
    databricks_spark_version="16.4.x-scala2.13",
    spark_configs={
        "fs.s3a.acl.default": "BucketOwnerFullControl",
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
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
offline_step_detection_dag >> rollout_sensor_task >> databricks_task >> final_dag_status_step
