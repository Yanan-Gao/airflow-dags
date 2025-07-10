import csv
import io
import json
import logging
from datetime import datetime, timedelta

import boto3
from airflow import AirflowException
from airflow.models import TaskInstance
from botocore.exceptions import ClientError
from airflow.models import Variable
from ttd.ttdenv import TtdEnvFactory
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from dags.pdg.data_subject_request.util import serialization_util
from dags.pdg.data_subject_request.dsr_item import DsrItem
from dags.pdg.data_subject_request.identity import DsrIdentityType

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

aws_conn_id = 'aws_default'

logs_bucket = 'thetradedesk-useast-logs-2'
dsr_notification_bucket = 'ttd-data-subject-requests-notification'
dsr_notification_prefix = 'notifications'
partner_dsr_logs_prefix = 'partnerdsr/collected'
enriched_csv_bucket = 'ttd-data-subject-requests'
dsar_backup_bucket = 'ttd-data-subject-requests-backup'
partner_dsr_enriched_prefix = 'partnerdsr/enriched'
user_dsr_prefix = 'dsar-data'
prod_prefix = 'env=prod'
test_prefix = 'env=test'

cold_storage_filename_prefix = 'ColdStorage_SegmentData'
targeting_data_filename = 'TargetingData.csv'

bucket_env_prefix = prod_prefix if is_prod else test_prefix


def generate_dated_folder_path(folder_prefix):
    stop_processing_date = datetime.now() - timedelta(days=1)
    stop_processing_formatted_date = stop_processing_date.strftime('%Y-%m-%d')
    begin_processing_date = stop_processing_date - timedelta(days=7)
    begin_processing_formatted_date = begin_processing_date.strftime('%Y-%m-%d')
    return f'{folder_prefix}/{begin_processing_formatted_date}to{stop_processing_formatted_date}'


# The initial cold storage segment pull is placed under the env=prod folder so that
# it can be accessed by the Sincera team for their work.
def _get_dsar_segment_path(jira_ticket: str) -> str:
    file_name = f'{cold_storage_filename_prefix}_{datetime.now().strftime("%Y%m%d")}.csv'
    return f'{bucket_env_prefix}/{user_dsr_prefix}/{jira_ticket.lower()}/{file_name}'


def _get_dsar_segment_backup_path(jira_ticket: str) -> str:
    file_name = f'{cold_storage_filename_prefix}_{datetime.now().strftime("%Y%m%d")}.csv'
    return f'{jira_ticket.lower()}/{file_name}'


def _get_bid_feedback_path(jira_ticket: str) -> str:
    return f'{user_dsr_prefix}/{jira_ticket.lower()}/BidFeedback.csv'


def _get_bid_feedback_backup_path(jira_ticket: str) -> str:
    return f'{jira_ticket.lower()}/BidFeedback_{datetime.now().strftime("%Y%m%d")}.csv'


def _get_targeting_data_path(jira_ticket: str) -> str:
    return f'{user_dsr_prefix}/{jira_ticket.lower()}/{targeting_data_filename}'


def _get_targeting_data_backup_path(jira_ticket: str) -> str:
    return f'{jira_ticket.lower()}/TargetingData_{datetime.now().strftime("%Y%m%d")}.csv'


def _get_user_data_path(jira_ticket: str, file_name: str) -> str:
    return f'{user_dsr_prefix}/{jira_ticket.lower()}/{file_name}'


def _get_user_data_path_backup(jira_ticket: str, file_name: str) -> str:
    return f'{jira_ticket.lower()}/{file_name}'


def put_enriched_object(csv_content: str):
    s3_client = AwsCloudStorage(conn_id=aws_conn_id)

    folder_name = f'{bucket_env_prefix}/{generate_dated_folder_path(partner_dsr_enriched_prefix)}'
    key = f'{folder_name}/advertiser_data_subject_requests.csv'

    try:
        s3_client.put_object(body=csv_content, bucket_name=enriched_csv_bucket, key=key)
    except ClientError as err:
        iam_user_identity = boto3.client('sts').get_caller_identity()
        logging.error(
            f"Caller Identity: {iam_user_identity}\n"
            f"Couldn't put enriched object CSV: {csv_content}\n"
            f"data into bucket {enriched_csv_bucket} with the following path {key}.\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


# Gets an S3 client for uploading DSAR files to the S3 backup. The S3 client is created
# using the special set of credentials that can be used to write to the backup bucket.
def _get_backup_client():
    aws_access_key_id = Variable.get('dsar_backup_aws_key')
    aws_secret_access_key = Variable.get('dsar_backup_aws_secret')

    session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    return session.client("s3")


# The backup bucket (also referred to as the "litigation hold" bucket) only applies to US-based requests.
def _put_data_backup(key: str, content: str, is_us: bool):
    if not is_us:
        return
    s3_client = _get_backup_client()
    try:
        s3_client.put_object(Body=content, Bucket=dsar_backup_bucket, Key=key)
    except ClientError as err:
        logging.error(
            f"Error uploading backup file."
            f"Code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


def _put_data(key: str, content: str, replace: bool = False):
    s3_client = AwsCloudStorage(conn_id=aws_conn_id)
    try:
        s3_client.put_object(body=content, bucket_name=enriched_csv_bucket, key=key, replace=replace)
    except ClientError as err:
        iam_user_identity = boto3.client('sts').get_caller_identity()
        logging.error(
            f"Caller Identity: {iam_user_identity}\n"
            f"Couldn't put content\n"
            f"data into bucket {enriched_csv_bucket} with the following path {key}.\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


def _put_data_from_file(local_path: str, bucket: str, key: str):
    s3_client = AwsCloudStorage(conn_id=aws_conn_id)
    try:
        s3_client.upload_file(key, local_path, bucket, True)
    except ClientError as err:
        logging.error(
            f"Exception putting user data for key: {key}\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


# Same comment as the _put_data_backup function. We need a different client with a different set of keys to write
# to the backup bucket. Only applies to US-based requests.
def _put_data_from_file_backup(local_path: str, bucket: str, key: str, is_us: bool):
    if not is_us:
        return
    s3_client = _get_backup_client()
    try:
        s3_client.upload_file(local_path, bucket, key)
    except ClientError as err:
        logging.error(
            f"Exception putting user data for key: {key}\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


def put_user_segments(csv_content: str, jira_ticket: str):
    key = _get_dsar_segment_path(jira_ticket)
    _put_data(key, csv_content, replace=True)


def put_user_segments_backup(csv_content: str, jira_ticket: str, is_us: bool):
    key = _get_dsar_segment_backup_path(jira_ticket)
    _put_data_backup(key, csv_content, is_us)


def put_bid_feedback_data(feedback_data: str, jira_ticket: str):
    key = _get_bid_feedback_path(jira_ticket)
    _put_data(key, feedback_data, replace=True)


def put_bid_feedback_data_backup(feedback_data: str, jira_ticket: str, is_us: bool):
    key = _get_bid_feedback_backup_path(jira_ticket)
    _put_data_backup(key, feedback_data, is_us)


def put_targeting_data(targeting_data: str, jira_ticket: str):
    key = _get_targeting_data_path(jira_ticket)
    _put_data(key, targeting_data, replace=True)


def put_targeting_data_backup(targeting_data: str, jira_ticket: str, is_us: bool):
    key = _get_targeting_data_backup_path(jira_ticket)
    _put_data_backup(key, targeting_data, is_us)


def put_user_data(local_path: str, jira_ticket: str, file_name: str):
    key = _get_user_data_path(jira_ticket, file_name)
    _put_data_from_file(local_path, enriched_csv_bucket, key)


def put_user_data_backup(local_path: str, jira_ticket: str, file_name: str, is_us: bool):
    key = _get_user_data_path_backup(jira_ticket, file_name)
    _put_data_from_file_backup(local_path, dsar_backup_bucket, key, is_us)


def get_targeting_data(jira_ticket: str):
    key = _get_targeting_data_path(jira_ticket)
    return _get_data(key)


def get_cold_storage_data(jira_ticket: str):
    s3_client = boto3.client('s3')
    prefix = f'{bucket_env_prefix}/{user_dsr_prefix}/{jira_ticket.lower()}/'
    try:
        response = s3_client.list_objects_v2(Bucket=enriched_csv_bucket, Prefix=prefix)
        contents = response.get('Contents')
        if not contents:
            message = f"""Unable to get the contents for {prefix}! If you're seeing this message, it means that the
                Cold Storage data has likely expired for this request. The retrieve_cold_storage_data task should be run
                again to generate the list of segments."""
            raise AirflowException(message)

        last_modified_file = max(contents, key=lambda f: f['LastModified'])
        return _get_data(last_modified_file['Key'])

    except ClientError as err:
        logging.error(
            f"Exception getting cold storage data:\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


def get_bidfedback_data(jira_ticket: str):
    key = _get_bid_feedback_path(jira_ticket)
    return _get_data(key)


def _key_exists(key: str) -> bool:
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=enriched_csv_bucket, Key=key)
        return True
    except ClientError:
        return False


def targeting_data_exists(jira_ticket: str) -> bool:
    key = _get_targeting_data_path(jira_ticket)
    return _key_exists(key)


def user_segments_exist(jira_ticket: str) -> bool:
    key = _get_dsar_segment_path(jira_ticket)
    return _key_exists(key)


def bid_feedback_exists(jira_ticket: str) -> bool:
    key = _get_bid_feedback_path(jira_ticket)
    return _key_exists(key)


def post_dsr_notification_to_s3(task_instance: TaskInstance, **kwargs):
    if not is_prod:
        return
    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    headers = ["UID2", "TDID", "EUID", "TDID"]

    identifiers_to_write = []
    for dsr_item in dsr_items:
        row_of_identifiers = []
        for identity in dsr_item.identities:
            if identity.identity_type == DsrIdentityType.UID2:
                row_of_identifiers.append(identity.identifier)
            if identity.identity_type == DsrIdentityType.EUID:
                row_of_identifiers.append(identity.identifier)
            row_of_identifiers.append(identity.tdid)

        identifiers_to_write.append(row_of_identifiers)

    csv_file = io.StringIO()
    csv_writer = csv.writer(csv_file)

    csv_writer.writerow(headers)
    csv_writer.writerows(identifiers_to_write)

    put_dsr_notification(csv_file.getvalue())

    return True


def put_dsr_notification(csv_content: str):
    """
    Posts dsr delete IDs to the notification bucket for partners to consume
    """
    s3_client = AwsCloudStorage(conn_id=aws_conn_id)

    folder_name = generate_dated_folder_path(dsr_notification_prefix)
    key = f'{folder_name}/data_subject_delete_request_ids.csv'

    try:
        s3_client.put_object(body=csv_content, bucket_name=dsr_notification_bucket, key=key)
    except ClientError as err:
        iam_user_identity = boto3.client('sts').get_caller_identity()
        logging.error(
            f"Caller Identity: {iam_user_identity}\n"
            f"Couldn't put dsr notification object CSV: {csv_content}\n"
            f"data into bucket {dsr_notification_bucket} with the following path {key}.\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        raise


def validate_opt_out_success():
    bucket_name = 'thetradedesk-datadelete-segmentsoptout'
    prefix = f'{prod_prefix}/transactions/DataSubjectRequest/'

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    folders = set()
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
        for common_prefix in page.get('CommonPrefixes', []):
            folders.add(common_prefix.get('Prefix'))

    if not folders:
        raise AirflowException(f'No transaction folders found after processing opt outs under s3 path {bucket_name}/{prefix}')

    finished_processing = True
    for folder in folders:
        finished_file_key = folder + '_FINISHED'

        try:
            response = s3.get_object(Bucket=bucket_name, Key=finished_file_key)
            print(f'Finished file found for {bucket_name}/{finished_file_key}')
            content = response['Body'].read().decode('utf-8')
            users_logs_result = json.loads(content)
            # Example
            # {
            #   "UsersLogsResult": {
            #     "transactions/DataSubjectRequest/{GUID}/Users/2.csv": {
            #       "Success": true,
            #       "SuccessfulLines": 1,
            #       "FailedLines": 0
            #     },
            #     "transactions/DataSubjectRequest/{GUID}/Users/1.csv": {
            #       "Success": true,
            #       "SuccessfulLines": 1,
            #       "FailedLines": 0
            #     }
            #   },
            #   "Status": "Success"
            # }
            status = users_logs_result.get('Status', '')
            if status != 'Success':
                finished_processing = False
                print(
                    f'Transaction {bucket_name}/{finished_file_key} has failed processing please investigate. '
                    f'HPC owns the processing. Reach out to them if applicable.'
                )
        except s3.exceptions.ClientError as e:
            print(f"{e.response}")
            finished_processing = False

    return finished_processing


def _get_data(key: str):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=enriched_csv_bucket, Key=key)
        return response['Body'].read()
    except ClientError as err:
        logging.error(
            f"Exception calling get_object on key: {key}\n"
            f"Error code: {err.response['Error']['Code']} and Error response: {err.response['Error']['Message']}"
        )
        return []
