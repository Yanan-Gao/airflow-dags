import logging
import os.path
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

THETRADEDESK_BI_BUCKET = "thetradedesk-bi"
OMNI_CHANNEL_OUTCOMES_PREFIX = "omni-channel-outcomes"
TTD_CTV_BUCKET = "ttd-ctv"
CHECK_DATA_SET_TASK_ID = "check_for_bi_dataset"
env_dir = "prod"
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_tags = OMNIUX.omniux().sub_team
    enable_slack_alert = True
else:
    slack_tags = None
    enable_slack_alert = False
    env_dir = "test"

jobQueueRootFolderPath = "job_queue/v=1"
queue_prefix = f"{OMNI_CHANNEL_OUTCOMES_PREFIX}/{env_dir}/{jobQueueRootFolderPath}"


def copy_file(storage: CloudStorage, src_bucket: str, src_file: str, dest_bucket: str, dest_prefix: str, move=False):
    dest_key = f"{dest_prefix}/{os.path.basename(src_file)}"
    storage.copy_file(src_key=src_file, src_bucket_name=src_bucket, dst_key=dest_key, dst_bucket_name=dest_bucket)
    logging.info(f"Copied: {src_bucket}/{src_file} -> {dest_bucket}/{dest_key}")
    if move:
        storage.remove_objects_by_keys(bucket_name=src_bucket, keys=[src_file])


def copy_files(src_bucket, src_prefix, dest_bucket, dest_prefix, move=False, name_filter=lambda x: True):
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    # List files in the source S3 bucket with the specified prefix
    files = filter(name_filter, aws_storage.list_keys(bucket_name=src_bucket, prefix=src_prefix))
    for file in files:
        copy_file(aws_storage, src_bucket, file, dest_bucket, dest_prefix, move)


def move_json_files(**kwargs):
    ds_date = kwargs['ti'].xcom_pull(task_ids=f'{CHECK_DATA_SET_TASK_ID}', key='available_date')
    src_prefix = f"{OMNI_CHANNEL_OUTCOMES_PREFIX}/{env_dir}/biJobs/date={ds_date.strftime('%Y%m%d')}"
    dest_prefix = f"{queue_prefix}/status=pending/user=bi"
    copy_files(TTD_CTV_BUCKET, src_prefix, TTD_CTV_BUCKET, dest_prefix, True, lambda x: x.endswith(".json"))


def copy_to_bi_jobs(**kwargs):
    ds_date = kwargs['ti'].xcom_pull(task_ids=f'{CHECK_DATA_SET_TASK_ID}', key='available_date')
    dest_prefix = f"{OMNI_CHANNEL_OUTCOMES_PREFIX}/{env_dir}/biJobs/date={ds_date.strftime('%Y%m%d')}"
    src_prefix = f"Path_to_Conversion_Input/date={ds_date.strftime('%Y%m%d')}"
    copy_files(THETRADEDESK_BI_BUCKET, src_prefix, TTD_CTV_BUCKET, dest_prefix)


class CustomDatasetSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, cloud_provider: CloudProvider = CloudProviders.aws, *args, **kwargs):
        super(CustomDatasetSensor, self).__init__(*args, **kwargs)
        self.cloud_provider = cloud_provider

    def poke(self, context):
        start_date = context.get('data_interval_end', datetime.today()).replace(day=1)
        logging.info(f"start date {start_date}")
        lookback = ((start_date + timedelta(days=31)).replace(day=1) - start_date).days
        logging.info(f"Poking for {start_date} with lookback {lookback}")
        cloud_storage = CloudStorageBuilder(self.cloud_provider).build()
        for day in range(lookback):
            check_date = start_date + timedelta(days=day)
            key = f"Path_to_Conversion_Input/date={check_date.strftime('%Y%m%d')}/_SUCCESS"
            logging.info(f"Checking for key {key} in bucket {THETRADEDESK_BI_BUCKET}")
            if cloud_storage.check_for_key(f"Path_to_Conversion_Input/date={check_date.strftime('%Y%m%d')}/_SUCCESS",
                                           THETRADEDESK_BI_BUCKET):
                logging.info(f"Found for date {check_date}.")
                context['ti'].xcom_push(key='available_date', value=check_date)
                return True
        return False


dag = TtdDag(
    dag_id="omnichannel-bi-job-mover",
    start_date=datetime(2024, 7, 25, 0, 0),
    schedule_interval='@monthly',
    retries=0,
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    tags=[OMNIUX.team.name, "omnichannel", "omnichannel-outcomes"],
    run_only_latest=True,
    dagrun_timeout=timedelta(days=30)
)

adag = dag.airflow_dag
check_data_set = OpTask(
    op=CustomDatasetSensor(
        task_id=CHECK_DATA_SET_TASK_ID,
        cloud_provider=CloudProviders.aws,
        mode='reschedule',
        timeout=30 * 24 * 60 * 60,  # Timeout after 30 days
        poke_interval=60 * 60,  # Check every hour
    )
)
copy_to_ctv_bucket_task = OpTask(op=PythonOperator(task_id='copy_to_ctv_bucket', python_callable=copy_to_bi_jobs, provide_context=True))
move_to_job_queue_task = OpTask(op=PythonOperator(task_id='move_to_job_queue', python_callable=move_json_files, provide_context=True))
dag >> check_data_set >> copy_to_ctv_bucket_task >> move_to_job_queue_task
