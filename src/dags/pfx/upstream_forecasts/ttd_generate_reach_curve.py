import logging
import boto3
from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.pfx.upstream_forecasts.ctv_cpm_adjustments import xcom_template_to_get_value
from dags.pfx.upstream_forecasts.upstream_utils import get_conf_or_default
from dags.tv.util.ttd_python_script_operator import run_python_app_in_emr_operator
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import PFX

# WARNING This DAG is deprecated, use ttd_generate_reach_curve_pyspark

CTV_S3_BUCKET = 'ttd-ctv'
UPSTREAM_FORECAST = 'upstream-forecast'
TAR_FILE_NAME = 'upstream.tar'
REACH_CURVE_FILE_NAME = 'reach_curves.snappy.parquet'

# This tar is a manual step to deploy into this folder in gitlab pipeline
TAR_FILE_S3 = f's3://{CTV_S3_BUCKET}/{UPSTREAM_FORECAST}/release/{TAR_FILE_NAME}'
# TAR_FILE_S3 = 's3://ttd-ctv/test/shawn.tang/lib/upstream.tar'

# Use --quick --output your.alias in command below to do testing and produce data in test/your.alias/ folder instead
APP_RUN_CMD = "python curve_generation.py --output PROD --fmap-version %s --static-data-date  %s --rundate %s --task-slice %s --skip-ctv-pp-curve %s"
TOTAL_SLICES = 8
MERGE_TASK_ID = f'merge-{TOTAL_SLICES}-slices-task-%d'
S3_SLICE_ROOT = f's3://{CTV_S3_BUCKET}/{UPSTREAM_FORECAST}/prod/slices/slice=%d/partner=master'
S3_MERGE_ROOT = f's3://{CTV_S3_BUCKET}/{UPSTREAM_FORECAST}/prod/partner=master'
LOGS_URI = f's3://{CTV_S3_BUCKET}/{UPSTREAM_FORECAST}/logs/'

dag_id = 'ttd-upstream-reach-curve'
#  TODO: change it back to monthly above once it matures
job_schedule_interval = None  # timedelta(days=7)  # Runs weekly for ensuring we have regular drops
job_start_date = datetime(2024, 7, 1)
default_static_data_date = "2023-02-13"
default_fmap_version = "v2"
calculate_dates_task_id = 'rc_gen_calculate_dates'

el_dorado_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "pfx", "reach curve"],
    max_active_runs=1
)

dag = el_dorado_dag.airflow_dag


def calculate_dates(**context):
    task_instance = context['task_instance']

    # extra_option
    extra_option = get_conf_or_default("extra_option", "", **context)
    logging.info(f'python reach curve gen extra options are: {extra_option}')
    task_instance.xcom_push(key="extra_option", value=extra_option)

    #  store fmap_version in xcom
    fmap_version = get_conf_or_default("fmap_version", default_fmap_version, **context)
    logging.info(f'fmap version is: {fmap_version}')
    task_instance.xcom_push(key="fmap_version", value=fmap_version)

    #  store static_data_date in xcom
    static_data_date_str = get_conf_or_default("static_data_date", default_static_data_date, **context)
    logging.info(f'static_date date is: {static_data_date_str}')
    task_instance.xcom_push(key="static_data_date", value=static_data_date_str)

    #  store run_date in xcom, which is the latest fmap date
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    if context['dag_run'].conf is None or context['dag_run'].conf.get("run_date") is None:
        #  check last date under s3a://ttd-ctv/upstream-forecast/data-preparation-v2/prod/frequencyDistributionsPartitioned/900/
        dag_run_date = datetime.strptime(context['ds'], '%Y-%m-%d')
        fmap_data = Datasources.ctv.get_upstream_forecast_fmap(fmap_version)
        fmap_date_str = fmap_data.check_recent_data_exist(
            cloud_storage=cloud_storage, ds_date=dag_run_date, max_lookback=15
        ).get().strftime("%Y-%m-%d")
    else:
        fmap_date_str = context['dag_run'].conf.get('run_date')
    logging.info(f'Using fmap date as run date {fmap_date_str}')
    task_instance.xcom_push(key="run_date", value=fmap_date_str)

    # store keys (reach-curve-target-date, demo-target-date) for downstream validation dag to use,
    # use run_date as fmap target-date and user profile second date as demo-target-date
    task_instance.xcom_push(key="reach-curve-target-date", value=fmap_date_str)
    if fmap_version == "v2":
        demo_weights = Datasources.ctv.upstream_forecast_demo_weights_v2
    else:
        demo_weights = Datasources.ctv.upstream_forecast_demo_weights_v3
    demo_date = demo_weights.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=datetime.strptime(fmap_date_str, "%Y-%m-%d"), max_lookback=15
    ).get().strftime("%Y-%m-%d")
    task_instance.xcom_push(key="demo-target-date", value=demo_date)


calculate_dates_step = PythonOperator(task_id=calculate_dates_task_id, python_callable=calculate_dates, dag=dag, provide_context=True)


def copy_slice_data(run_date: str, slice_number: int, fmap_version: str):
    s3_client = boto3.client('s3')
    if fmap_version == "v2":
        root_path = UPSTREAM_FORECAST
    else:
        root_path = f"{UPSTREAM_FORECAST}/data-preparation-v3"

    # Define the source and destination object key
    source_object_key = f'{root_path}/prod/slices/slice={slice_number}/partner=master/date={run_date}/{REACH_CURVE_FILE_NAME}'
    destination_object_key = f'{root_path}/prod/partner=master/date={run_date}/slice={slice_number}/{REACH_CURVE_FILE_NAME}'

    logging.info(f'copying from {source_object_key} to {destination_object_key}')

    # Perform the S3 copy operation
    s3_client.copy_object(CopySource={'Bucket': CTV_S3_BUCKET, 'Key': source_object_key}, Bucket=CTV_S3_BUCKET, Key=destination_object_key)


# Define the BranchOperator to call TriggerDagRunOperator conditionally based on conf flag trigger_downstream_dag
def create_dag_run(context, dag_run_obj):
    task_instance = context['task_instance']
    logging.info(str(task_instance))
    reach_curve_target_date = task_instance.xcom_pull(dag_id=dag_id, task_ids=calculate_dates_task_id, key="reach-curve-target-date")
    demo_target_date = task_instance.xcom_pull(dag_id=dag_id, task_ids=calculate_dates_task_id, key="demo-target-date")
    logging.info(f'reach-curve-target-date:{reach_curve_target_date} and demo-target-date:{demo_target_date}')
    dag_run_obj.payload = {"reach-curve-target-date": reach_curve_target_date, "demo-target-date": demo_target_date}
    return dag_run_obj


trigger_reach_curve_validation_task = TriggerDagRunOperator(
    task_id="trigger_reach_curve_validation_task",
    trigger_dag_id="ctv-upstream-forecast-validation",
    conf={
        "reach-curve-target-date": xcom_template_to_get_value(dag_id, calculate_dates_task_id, "reach-curve-target-date"),
        "demo-target-date": xcom_template_to_get_value(dag_id, calculate_dates_task_id, "demo-target-date")
    }
)


def should_trigger_downstream_reach_curve_validation(**context):
    # if it is auto scheduled, we trigger downstream
    is_scheduled = not context['dag_run'].external_trigger
    logging.info(f'is auto scheduled:{str(is_scheduled)}')
    if is_scheduled:
        return trigger_reach_curve_validation_task.task_id

    # manually scheduled with conf flag enabled, we trigger downstream
    conf_value = context['dag_run'].conf.get('trigger-downstream-dag')
    if conf_value == 'True':
        return trigger_reach_curve_validation_task.task_id
    else:
        return do_nothing_task.task_id


branch_op = BranchPythonOperator(
    task_id='branch_task', provide_context=True, python_callable=should_trigger_downstream_reach_curve_validation, dag=dag
)

do_nothing_task = EmptyOperator(task_id='do_nothing_task', dag=dag)

for i in range(1, TOTAL_SLICES + 1):
    run_date = xcom_template_to_get_value(dag_id, calculate_dates_task_id, "run_date")
    static_data_date = xcom_template_to_get_value(dag_id, calculate_dates_task_id, "static_data_date")
    fmap_version_val = xcom_template_to_get_value(dag_id, calculate_dates_task_id, "fmap_version")
    extra_option_val = xcom_template_to_get_value(dag_id, calculate_dates_task_id, "extra_option")

    emr_ops = run_python_app_in_emr_operator(
        dag=el_dorado_dag.airflow_dag,
        application_name=f'CTV-upstream-reach-curve-gen-{i}',
        process_name="Airflow: ctv-generate-reach-curve "
        f"{run_date} - Part: {i}/{TOTAL_SLICES}",
        s3_tar_file_path=TAR_FILE_S3,
        app_run_cmd=APP_RUN_CMD % (fmap_version_val, static_data_date, run_date, f'{i}/{TOTAL_SLICES}', extra_option_val),
        instance_type=R5d.r5d_24xlarge(),
        logs_uri=LOGS_URI,
        cluster_tags={'Team': 'TV'}
    )

    merge_op = PythonOperator(
        dag=el_dorado_dag.airflow_dag,
        task_id=MERGE_TASK_ID % i,
        python_callable=copy_slice_data,
        op_kwargs={
            'run_date': run_date,
            'slice_number': i,
            'fmap_version': fmap_version_val
        }
    )

    calculate_dates_step >> emr_ops.upstream_list[0]
    emr_ops >> merge_op >> branch_op >> [trigger_reach_curve_validation_task, do_nothing_task]
