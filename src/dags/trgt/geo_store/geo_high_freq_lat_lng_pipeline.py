from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils
from ttd.slack.slack_groups import targeting
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory
from airflow.operators.python import PythonOperator
from ttd.tasks.op import OpTask
import boto3
import io
import zipfile
import re

job_schedule_interval = '0 4 * * *'
job_start_date = datetime(2025, 4, 20)

mm_source_bucket = 'ttd-geo'
mm_source_prefix = 'Maxmind'

geostore_bucket = 'thetradedesk-useast-geostore'
maxmind_dataset_prefix_key = 'maxmindDatasetPrefix'
avails_end_date_key = 'availsEndDate'
avails_lookback_days_key = 'availsLookbackDays'
avails_lookback_days_value = 7
avails_high_freq_lat_lng_threshold_key = 'availsHighFreqLatLngThreshold'
avails_high_freq_lat_lng_threshold_value = 100

dag_id = 'geo-high-freq-lat-lng-pipeline'
extract_maxmind_dataset_task_id = 'extract_maxmind_dataset_task'
get_end_date_and_lookback_days_task_id = 'get_end_date_and_lookback_days_task'
get_diff_cache_latest_version_task_id = 'get_diff_cache_latest_version_task'

# enable this when both old and new delta distributors can read the diff cache file
update_diff_cache_version_enabled = True

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    executable_path = 's3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-kzh-trgt-1351-high-freq-write-diff-cache-assembly.jar'
    env = 'test'
else:
    executable_path = 's3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar'
    env = 'prod'

# environment related config
avails_high_freq_lat_lng_input_prefix_key = 'availsHighFreqLatLngInputPrefix'
avails_high_freq_lat_lng_input_prefix_value = f'env={env}/highfreqv2/sampled-avails-raw/ExportAvailsLatLng/VerticaAws'
high_freq_lat_lng_dataset_merged_prefix_key = 'highFreqDatasetMergedPrefix'
high_freq_lat_lng_dataset_merged_prefix_value = f'env={env}/highfreqv2/merged'
high_freq_lat_lng_dataset_diff_cache_prefix_key = 'highFreqDatasetDiffCachePrefix'
high_freq_lat_lng_dataset_diff_cache_prefix_value = f'env={env}/highfreqv2/diffcache'

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    run_only_latest=True,
    max_active_runs=1,
    slack_channel='#scrum-targeting-alarms',
    slack_tags=targeting,
    slack_alert_only_for_prod=True,
    tags=[targeting.jira_team]
)


def extract_maxmind_dataset(destination_bucket, destination_prefix, **context):
    mm_ent_csv_keys = list_all_objects_with_prefix(mm_source_bucket, mm_source_prefix, 'GeoIP2-Enterprise-CSV')
    max_version_key, max_version = get_max_version_key(mm_ent_csv_keys)
    print(max_version_key, max_version)
    maxmind_dataset_prefix = f"{destination_prefix}/version={max_version}"
    context['task_instance'].xcom_push(key=maxmind_dataset_prefix_key, value=maxmind_dataset_prefix)
    if len(list_all_objects_with_prefix(destination_bucket, maxmind_dataset_prefix)) > 0:
        print('The latest Maxmind dataset has been processed')
        return
    print(f'Processing {max_version_key}')
    s3 = boto3.client('s3')
    zip_obj = s3.get_object(Bucket=mm_source_bucket, Key=max_version_key)
    zip_content = zip_obj['Body'].read()
    zip_bytes = io.BytesIO(zip_content)
    with zipfile.ZipFile(zip_bytes, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if 'GeoIP2-Enterprise-Blocks-IP' in file_info.filename:
                csv_content = zip_ref.read(file_info.filename)
                filename = file_info.filename.split('/')[-1]
                destination_key = f'{maxmind_dataset_prefix}/{filename}'
                s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=csv_content)
                print(f'Extracted {filename} to {destination_key}')


def get_end_date_and_lookback_days(**context):
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
        # data on 20250417 has been copied to
        # env=test/highfreqv2/sampled-avails-raw/ExportAvailsLatLng/VerticaAws/date=20250417/
        end_date = datetime(2025, 4, 17)
        lookback_days = 1
    else:
        end_date = datetime.utcnow() - timedelta(days=1)
        # use the date of most recently finished vertica export task as end date
        while not has_object(geostore_bucket, get_success_object_key(end_date)):
            end_date -= timedelta(days=1)
        # starting from the beginning of the lookback window
        # find the first date that has a successful vertica export task
        lookback_days = avails_lookback_days_value
        while not has_object(geostore_bucket, get_success_object_key(end_date - timedelta(days=lookback_days - 1))):
            lookback_days -= 1
    context['task_instance'].xcom_push(key=avails_end_date_key, value=end_date.strftime('%Y%m%d'))
    context['task_instance'].xcom_push(key=avails_lookback_days_key, value=lookback_days)


def get_success_object_key(date):
    return f"{avails_high_freq_lat_lng_input_prefix_value}/date={date.strftime('%Y%m%d')}/_SUCCESS"


def update_diff_cache_version(conn_id):
    latest_version = GeoStoreUtils.get_max_number(geostore_bucket, high_freq_lat_lng_dataset_diff_cache_prefix_value)
    print(f'Latest version: {latest_version}')
    if env == 'prod':
        # copy the diff cache object to the old location to keep old delta distributor running
        s3 = boto3.client('s3')
        source_key = f'{high_freq_lat_lng_dataset_diff_cache_prefix_value}/{latest_version}.full'
        dest_key = f'highfreq/{latest_version}.full'
        copy_source = {'Bucket': geostore_bucket, 'Key': source_key}
        s3.copy(copy_source, geostore_bucket, dest_key)
        print(f'Copied {source_key} to {dest_key}')
    if update_diff_cache_version_enabled:
        GeoStoreUtils.update_high_freq_diff_cache_index_db(conn_id, latest_version)
        print(f'High freq diff cache version has been updated to {latest_version}')
    else:
        print('No database update as this task is not enabled')


def has_object(bucket_name, object_key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except Exception:
        return False


def list_all_objects_with_prefix(bucket, prefix, keyword=None):
    s3 = boto3.client('s3')
    continuation_token = None
    object_keys = []

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        object_keys.extend([obj['Key'] for obj in response.get('Contents', []) if keyword is None or keyword in obj['Key']])

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return object_keys


def get_max_version_key(keys):
    max_version = -1
    max_version_key = None

    pattern = re.compile(r'GeoIP2-Enterprise-CSV_(\d+)\.zip$')

    for key in keys:
        match = pattern.search(key)
        if match:
            version = int(match.group(1))
            if version > max_version:
                max_version = version
                max_version_key = key

    return max_version_key, max_version


def get_xcom_value(dag_id, task_id, key):
    return f"{{{{ task_instance.xcom_pull(dag_id='{dag_id}', task_ids='{task_id}', key='{key}') }}}}"


extract_maxmind_dataset_task = OpTask(
    op=PythonOperator(
        task_id=extract_maxmind_dataset_task_id,
        python_callable=extract_maxmind_dataset,
        op_kwargs={
            'destination_bucket': geostore_bucket,
            'destination_prefix': f'env={env}/highfreqv2/Maxmind'
        },
        provide_context=True
    )
)

get_end_date_and_lookback_days_task = OpTask(
    op=PythonOperator(task_id=get_end_date_and_lookback_days_task_id, python_callable=get_end_date_and_lookback_days, provide_context=True)
)

# the pipeline is not compute intensive, so start with smaller spec
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_xlarge().with_fleet_weighted_capacity(1)],
    spot_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_xlarge().with_fleet_weighted_capacity(1)],
    spot_weighted_capacity=4,
)

cluster_task = EmrClusterTask(
    name='geo-high-freq-lat-lng-pipeline-cluster',
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={'Team': targeting.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

generate_high_freq_lat_lng_job_task = EmrJobTask(
    name='generate-high-freq-lat-lng-job-task',
    class_name='com.thetradedesk.jobs.highfreq.GenerateHighFreqLatLng',
    executable_path=executable_path,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        (maxmind_dataset_prefix_key, get_xcom_value(dag_id, extract_maxmind_dataset_task_id, maxmind_dataset_prefix_key)),
        (avails_end_date_key, get_xcom_value(dag_id, get_end_date_and_lookback_days_task_id, avails_end_date_key)),
        (avails_lookback_days_key, get_xcom_value(dag_id, get_end_date_and_lookback_days_task_id, avails_lookback_days_key)),
        (avails_high_freq_lat_lng_threshold_key, avails_high_freq_lat_lng_threshold_value),
        (avails_high_freq_lat_lng_input_prefix_key, avails_high_freq_lat_lng_input_prefix_value),
        (high_freq_lat_lng_dataset_merged_prefix_key, high_freq_lat_lng_dataset_merged_prefix_value),
        (high_freq_lat_lng_dataset_diff_cache_prefix_key, high_freq_lat_lng_dataset_diff_cache_prefix_value)
    ]
)

update_diff_cache_version_task = OpTask(
    op=PythonOperator(
        task_id='update-diff-cache-version-task',
        python_callable=update_diff_cache_version,
        op_kwargs={'conn_id': 'ttd_geo_provdb'},
        provide_context=True
    )
)

cluster_task.add_parallel_body_task(generate_high_freq_lat_lng_job_task)

dag >> extract_maxmind_dataset_task >> get_end_date_and_lookback_days_task >> cluster_task >> update_diff_cache_version_task
adag = dag.airflow_dag
