import logging
import json
import gzip
import boto3

from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.cloud_provider import CloudProviders

FORECASTING_BUCKET = 'ttd-forecasting-useast'
forecast_aws_connection_id = 'forecast_aws_connection'


def get_s3_connection():
    try:
        logging.info(f"Getting aws connection for conn id: {forecast_aws_connection_id}")
        s3 = CloudStorageBuilder(CloudProviders.aws).set_conn_id(forecast_aws_connection_id).build()
        return s3
    except Exception as e:
        logging.error(f"Failed getting aws connection for conn id {forecast_aws_connection_id} with exception: {e}")
        raise


def download_single_json_from_s3(bucket_name, file_key, is_gzipped=False):
    import pandas as pd

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)

    if is_gzipped:
        file_content = gzip.decompress(obj['Body'].read()).decode('utf-8')
    else:
        file_content = obj['Body'].read().decode('utf-8')

    try:
        # Try NDJSON first
        json_lines = [json.loads(line) for line in file_content.strip().split('\n') if line]
        df = pd.DataFrame(json_lines)
    except json.JSONDecodeError:
        # Fallback to full JSON
        parsed_json = json.loads(file_content)
        if isinstance(parsed_json, list):
            df = pd.DataFrame(parsed_json)
        else:
            df = pd.DataFrame([parsed_json])

    return df


def download_json_from_s3(bucket_name, prefix):
    try:
        import pandas as pd

        logging.info("Getting s3 boto client.")
        s3 = boto3.client('s3')

        logging.info(f"List all JSON files in {bucket_name}, {prefix}.")
        s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        json_files = [obj['Key'] for obj in s3_objects.get('Contents', []) if obj['Key'].endswith('.json.gz')]

        # Load JSON files into a single DataFrame
        df_list = []

        logging.info("Reading json files into panda DFs.")
        for file_key in json_files:
            df = download_single_json_from_s3(bucket_name, file_key, True)
            df_list.append(df)

        logging.info("Concatenating all DFs.")
        final_df = pd.concat(df_list) if df_list else pd.DataFrame()

        return final_df
    except Exception as e:
        logging.error(f"Failed downloading json from {bucket_name}, {prefix} with error: {e}")
        raise


def save_json_to_s3(json_data, key: str, bucket_name=FORECASTING_BUCKET):
    try:
        s3 = get_s3_connection()
        logging.info(f"Writing JSON to S3 bucket {bucket_name} and key {key}")
        s3.put_object(body=json_data, bucket_name=bucket_name, key=key, replace=True)
    except Exception as e:
        logging.error(f"Failed to save json to s3: {e}")
        raise
