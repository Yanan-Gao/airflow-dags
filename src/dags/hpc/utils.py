# Checks whether to push to SQL DB
import logging
from datetime import datetime, timezone

import boto3

from dags.hpc import constants
from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from enum import StrEnum, auto


class CrossDeviceLevel(StrEnum):
    DEVICE = auto()
    PERSON = auto()
    HOUSEHOLD = auto()
    HOUSEHOLDEXPANDED = auto()


class Source(StrEnum):
    MAIN = auto()
    IPAWS = auto()
    CHINA = auto()


class DataType(StrEnum):
    TARGETINGDATA = auto()
    METADATA = auto()


def is_push_to_sql_enabled(**kwargs):
    return kwargs['push_to_sql_enabled']


# Checks whether to run Vertica Load
def is_vertica_load_enabled(**kwargs):
    return kwargs['verticaload_enabled']


# Gets vertica load objects for specified path
def get_vertica_load_object_list(**kwargs):
    aws_cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    run_date = kwargs['datetime']
    date_str = datetime.strptime(run_date, "%Y-%m-%dT%H:00:00").strftime("%Y%m%d")
    hour_str = datetime.strptime(run_date, "%Y-%m-%dT%H:00:00").strftime("%-H")
    now_utc = datetime.now(timezone.utc)
    logfiletask_endtime = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
    s3_prefix = f"{kwargs['s3_prefix']}date={date_str}/hour={hour_str}/"
    log_type_id = kwargs['log_type_id']
    keys = aws_cloud_storage.list_keys(bucket_name=constants.DMP_ROOT, prefix=s3_prefix)
    if keys is None or len(keys) == 0:
        raise Exception(f'Expected non-zero number of files for VerticaLoad log type {log_type_id} at s3 path {s3_prefix}')
    logging.info(f'Found {len(keys)} files for log type {log_type_id}')
    object_list = []
    for key in keys:
        object_list.append((log_type_id, key, date_str, 1, 0, 1, 1, 0, logfiletask_endtime)
                           )  # CloudServiceId 1 == AWS, DataDomain 1 == TTD_RestOfWorld

    return object_list


def is_sqs_queue_empty(queue_name: str, region_name: str, check_visible_messages_only: bool = False) -> bool:
    session = boto3.session.Session()
    client = session.client(service_name='sqs', region_name=region_name)
    queue_attributes = client.get_queue_attributes(
        QueueUrl=queue_name,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
    )['Attributes']

    msg_num = int(queue_attributes['ApproximateNumberOfMessages'])
    msg_num_non_visible = int(queue_attributes['ApproximateNumberOfMessagesNotVisible'])
    msg_num_delayed = int(queue_attributes['ApproximateNumberOfMessagesDelayed'])

    print("Message nums: ", (msg_num, msg_num_non_visible, msg_num_delayed))
    if check_visible_messages_only:
        return msg_num == 0
    else:
        return all(x == 0 for x in (msg_num, msg_num_non_visible, msg_num_delayed))


def write_empty_success_file(cloud_provider: CloudProvider, bucket_name: str, folder_prefix: str, override=True):
    folder_prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'

    cloud_storage_client = CloudStorageBuilder(cloud_provider).build()
    cloud_storage_client.put_object(bucket_name=bucket_name, key=f"{folder_prefix}_SUCCESS", body="", replace=override)
