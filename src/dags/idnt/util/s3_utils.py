from datetime import datetime, timedelta, timezone
import re
import boto3
import logging
from typing import Optional, Tuple, Callable, List
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage


def split_s3_path(path: str) -> Tuple[str, str]:
    """Split an S3-style URL into its components (bucket and key/prefix).

    Args:
        path (str): An S3-style URL.

    Returns:
        Tuple[str, str]: A tuple containing the bucket name and the rest of the path.
    """
    pattern = re.compile(r"s3:\/\/([a-zA-Z0-9\-\.]{3,55})\/(.*)")

    if (match := re.search(pattern, path)) is None:
        raise ValueError(f"Incorrect path: {path}")

    return match.group(1), match.group(2)


def extract_date_from_path(path: str, date_format: str = '%Y-%m-%d') -> Optional[datetime]:
    """Extract the date from the given s3 path in the date_format passed.

    Args:
        path (str): An S3-style path
        date_format (str): The date format we want to find the date in.
    Returns:
        datetime: A datetime object of the date in the path
    """
    # Map format to regex date patterns
    format_patterns = {
        "%Y-%m-%d": r"(\d{4}-\d{2}-\d{2})",  # Matches '2025-01-01'
        "%Y%m%d": r"(\d{8})",  # Matches '20250101'
    }

    pattern = format_patterns.get(date_format)
    if not pattern:
        raise ValueError(f"Unsupported date format: {date_format}")

    if match := re.search(pattern, path):
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            raise ValueError(f"Date string '{date_str}' does not match the expected format.")

    return None


# TODO reduce the complexity of this function, one way is to use 'awswrangler' library
def count_number_of_nested_folders_in_prefix(bucket, prefix, execute_date, search_success_prefix):
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
    number_of_folders = 0
    max_look_back = 31
    output_data = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')

    logging.info(f"Checking number of paths in: {bucket}/{prefix} for date: {execute_date}")

    for outputPrefixes in output_data.search('CommonPrefixes'):
        if outputPrefixes and outputPrefixes.get('Prefix', None):
            output_date_folder = outputPrefixes.get('Prefix')
            # We want to extract the date from that string
            logging.info("Looking at folder: " + output_date_folder)
            current_folder_date_time = extract_date_from_path(output_date_folder)
            if current_folder_date_time is None:
                continue
            is_after_look_back = current_folder_date_time.date() >= execute_date.date() - timedelta(days=max_look_back)
            if is_after_look_back:
                # need to page through these date folders too. But we only need to check one level deep
                folders_in_date = paginator.paginate(
                    Bucket=bucket, Prefix=prefix + current_folder_date_time.strftime('%Y-%m-%d') + "/", Delimiter='/'
                )
                for folderPrefixes in folders_in_date.search('CommonPrefixes'):
                    if search_success_prefix:
                        success_iterator = paginator.paginate(Bucket=bucket, Prefix=folderPrefixes['Prefix'], Delimiter='_SUCCESS')
                        for success_prefix in success_iterator:
                            for iterating_files in success_prefix['CommonPrefixes']:
                                success_file_path = iterating_files['Prefix']
                                if ('_SUCCESS' in success_file_path and 'matchRates' not in success_file_path
                                        and 'precisions' not in success_file_path):
                                    # We have one _SUCCESS file for total success
                                    number_of_folders += 1
                    else:
                        number_of_folders += 1

    logging.info(f"Found folders: {number_of_folders}")
    return number_of_folders


def get_latest_existing_date(dates: List[datetime], date_to_path: Callable[[datetime], str]) -> Optional[datetime]:
    """Return the latest existing date from a list of dates given a conversion function to convert date to a data path.

    Args:
        dates (List[datetime]): A list of datetime objects to check for existence on S3.
        date_to_path (Callable[[datetime], str]): A callable that takes a datetime object and returns the corresponding S3 URL.

    Returns:
        datetime: The latest existing date from the provided list of dates. Returns None if it's not found.
    """

    dates_to_check = sorted(dates, reverse=True)

    for d in dates_to_check:
        path = date_to_path(d)
        logging.info(f"Checking path at: {path} for {d}")
        if does_folder_have_objects(path):
            logging.info(f"Latest date found: {d}")
            return d

    return None


def does_folder_have_objects(path: str) -> bool:
    """Check if a folder has objects on S3.

    Args:
        path (str): An S3-style URL representing the path of the folder.

    Returns:
        bool: A boolean indicating whether the folder had blobs in it or not.
    """

    s3_hook = AwsCloudStorage(conn_id='aws_default')
    bucket, prefix = split_s3_path(path.rstrip("/") + "/")

    if s3_hook.list_keys(bucket_name=bucket, prefix=prefix):
        logging.info(f"Folder exists: {path}")
        return True
    else:
        logging.info(f"Folder doesn't exist: {path}")
        return False


def does_folder_have_specific_objects(path: str, files_to_check: List[str] = []) -> bool:
    """Check if a folder has specific objects on S3.

    Args:
        path (str): An S3-style URL representing the path of the folder.

    Returns:
        bool: A boolean indicating whether the folder had blobs in it or not.
    """

    s3_hook = AwsCloudStorage(conn_id='aws_default')
    bucket, prefix = split_s3_path(path.rstrip("/") + "/")

    keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)

    if not keys:
        logging.info(f"Folder doesn't exist: {path}")
        return False

    for file in files_to_check:
        if not any(key.endswith(file) for key in keys):
            logging.info(f"File: {file} doesn't exist in path: {path}")
            return False
    return True


def get_latest_etl_date_from_path(
    s3_hook: AwsCloudStorage,
    output_path: str,
    date_from: datetime = datetime.today(),
    lookback_days: int = 90,
    date_format: str = "%Y-%m-%d"
) -> datetime:
    """Extract the latest date from the output path passed.

    Args:
        s3_hook: An AWS Hook for S3 path
        output_path (str): An S3 output path
        date_from (date): From which date onwards to check for the latest date in the path
        lookback_days (int): The number of days to lookback on, if there is no latest date found
        date_format (str): The date format in the output path
    Returns:
        datetime: A datetime object of the latest date in the path
    """

    output_bucket, output_prefix = split_s3_path(output_path.rstrip("/") + "/")
    prefixes_found = s3_hook.list_prefixes(output_bucket, output_prefix)

    # find dates in the prefixes_found, filter out None values (where path didn't match the format)
    dates = [d for d in [extract_date_from_path(path, date_format) for path in prefixes_found] if d is not None]

    # check for the most recent ETL date, set a lookback window of 90 days otherwise
    if prefixes_found and dates:
        last_etl_date = max(dates)
    else:
        last_etl_date = date_from - timedelta(days=lookback_days)

    return last_etl_date.replace(tzinfo=timezone.utc)


def all_trigger_files_complete(
    date_from: datetime, date_to: datetime, date_to_path: Callable[[datetime], str], trigger_files: List[str]
) -> bool:
    """
        It is insufficient to only check latest path is non-empty for liveramp delivery
        as different id types have trigger files to confirm delivery their delivery which can also spill across multiple dates
        hence overall input delivery is only complete once triggers for all valid id types are confirmed
    Args:
        date_from: start date to begin check of trigger files from
        date_to: end date [not inclusive]
        date_to_path: Function to generate path given a date

    Returns:
        True/False if all trigger files exist since last delivery/etl

    """
    dates_to_check = sorted([date_from - timedelta(days=i) for i in range((date_from - date_to).days)], reverse=True)

    for trigger_file in trigger_files:
        if not any(does_folder_have_specific_objects(date_to_path(d), [trigger_file]) for d in dates_to_check):
            logging.info(f"Trigger file: {trigger_file} doesn't exist b/w dates:  {date_from} inclusive and {date_to} exclusive")
            return False
    logging.info(f"Trigger files: {trigger_files} found b/w dates:  {date_from} inclusive and {date_to} exclusive")
    return True
