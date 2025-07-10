"""
Airflow DAG to enumerate logs from SumoLogic and store the acquired object's FeedId, LastAccessedDate, and
LastAccessedUser into reds.FeedAccessHistory and exposure.FeedAccessHistory tables.

The general process is as follows:
1. Pull SumoLogs "GetObject" logs and store it in a dictionary, of Filepath --> (DateTime, User)
2. Pulls all <DestinationLocation> from the reds and exposure feed tables and build a trie data structure, where
   traversal is based on <DestinationLocation>. If traversal is successful, gets a FeedId.
3. Compare the Filepaths found in the SumoLogic logs and the filepaths in the trie. Create a new dictionary of
   FeedId --> (DateTime, User)
4. Store the FeedId, LastAccessedDate, and LastAccessedUser into reds.FeedAccessHistory and exposure.FeedAccessHistory
   tables

More Details: https://atlassian.thetradedesk.com/confluence/display/EN/Deprecate+Unused+REDS+Feeds
"""

import asyncio
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, List
import boto3
import time
import pymssql

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from dags.puma.reds.helpers import redsfeed, exposurefeed
from dags.puma.reds.helpers.trie import FilePathTrie
from dags.puma.reds.helpers.sumodata import SumoData
from ttd.tasks.op import OpTask
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import puma
from ttd.ttdenv import TtdEnvFactory
from ttd.workers.worker import Workers
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

from ttd.ttdprometheus import get_or_register_gauge, push_all
from prometheus_client.metrics import Gauge

from sumologic import SumoLogic

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False
connection_string_provisioning = "Prod_Provisioning" if is_prod else "IntSb_Provisioning"
connection_string_schedreporting = "Prod_SchedReporting" if is_prod else "IntSb_SchedReporting"
version = "Prod" if is_prod else "Test"

aggregatedfilepattern1 = r'(\d{4}-\d{2}-\d{2})(\d{1,2})([a-zA-Z]+)(\d)(\d+)(\d)'
aggregatedfilepattern2 = r'([a-zA-Z]+)_(\d+)_(\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2})_([a-f0-9]{32})'
aggregatedfilepattern3 = r'_(\d+)_([a-f0-9]{32})'

job_name = 'reds_update_last_accessed_feeds'
job_description = 'Reds Update Last Accessed Feeds'
dag = TtdDag(
    dag_id=f"{job_name}_backfill_90",
    start_date=datetime(2024, 4, 10, 0, 0, 0, 0),
    end_date=datetime(2024, 6, 2, 0, 0, 0, 0),
    run_only_latest=False,
    schedule_interval="*/15 * * * *",
    slack_channel=puma.alarm_channel,
    tags=[puma.name, puma.jira_team],
    retries=3,
    retry_delay=(timedelta(minutes=15)),
    max_active_runs=3
)

adag = dag.airflow_dag  # MUST be explicitly defined for Airflow to parse this file as a DAG


async def store_sumo_logs(execution_datetime) -> Dict[str, SumoData]:
    logging.info("Started function store_sumo_logs()")
    start_time = time.time()

    logging.info("Connecting to SumoLogic...")
    conn = BaseHook.get_connection('sumo_last_accessed_reds')
    access_id = conn.login
    access_key = conn.password
    sumo = SumoLogic(access_id, access_key)
    logging.info("Successfully connected to SumoLogic!")

    excluded_fields = [
        '_raw', '_messageid', '_sourceid', '_sourcename', '_sourcehost', '_sourcecategory', '_format', '_size', '_receipttime',
        '_messagecount', '_source', '_collectorid', '_collector', '_blockid'
    ]
    # Generate the query string with excluded fields
    excluded_fields_string = ', '.join(excluded_fields)
    query = rf"""
        ((_sourceCategory=aws/org/cloudtrail (_dataTier=Continuous OR (_dataTier=Infrequent and _index=aws_org_logs_infrequent)) "thetradedesk-useast-partner-datafeed" "GetObject"))
        | json field=_raw "resources[0].ARN" as arn
        | json field=_raw "userIdentity.accountId" as accountId
        | json field=_raw "userIdentity.principalId" as principalId
        | json field=_raw "errorCode" as errorCode nodrop
        | where eventName="GetObject"
        | parse regex field=arn "arn:aws:s3:::(?<parsed_arn>(?s).*)" nodrop
        | where !(parsed_arn matches /.*\.tsv/ or parsed_arn matches /.*\.parquet/)
        | where isNull(errorCode)  // Filter out logs with an errorCode
        | fields parsed_arn, accountId, principalId
        | fields - {excluded_fields_string}
        """
    try:
        start_datetime = execution_datetime - timedelta(minutes=15)
        formatted_execution_datetime = execution_datetime.strftime("%Y%m%d%H%M")
        formatted_start_datetime = start_datetime.strftime("%Y%m%d%H%M")
        logging.info(f"Running the following query {query} with timespan from {formatted_start_datetime} to {formatted_execution_datetime}")
        logging.info(f"fromTime: {start_datetime.timestamp() * 1000}, toTime: {execution_datetime.timestamp() * 1000} in Epoch Time")

        loop = asyncio.get_event_loop()
        search_job = await loop.run_in_executor(
            None, sumo.search_job, query, int(start_datetime.timestamp() * 1000), int(execution_datetime.timestamp() * 1000), 'UTC', False,
            'Manual'
        )
        logging.info(f"Search_job link: {search_job}")
        status = await loop.run_in_executor(None, sumo.search_job_status, search_job)
    except Exception as e:
        logging.error(f"Error starting sumo search job: {e} for query {query}")
        raise

    counter = 0
    logging.info("Waiting for sumo logic query to finish processing")
    while status["state"] != "DONE GATHERING RESULTS":
        if status["state"] == 'CANCELLED' or counter >= 60 * 60 * 3:
            raise Exception("Sumo query timed out or was cancelled")
        counter += 1
        await asyncio.sleep(1)
        status = await loop.run_in_executor(None, sumo.search_job_status, search_job)

    logging.info("Storing Sumo Logs in Dictionary (Batched)")
    offset = 0
    limit = 10000
    counter = 0
    filepath_to_timestamp_user: Dict[str, SumoData] = {}
    if status['state'] == 'DONE GATHERING RESULTS':
        response = await loop.run_in_executor(None, sumo.search_job_messages, search_job, limit, offset)
        while len(response["messages"]) != 0:
            for r in response["messages"]:
                filepath = r["map"]["parsed_arn"]
                account_id = r["map"]["accountid"]
                principal_id = r["map"]["principalid"].split(':')[0]
                sumo_timestamp = r["map"]["_messagetime"]
                timestamp = datetime.fromtimestamp(int(sumo_timestamp) / 1000, tz=timezone.utc)

                if filepath not in filepath_to_timestamp_user or timestamp > filepath_to_timestamp_user[filepath].last_accessed_datetime:
                    filepath_to_timestamp_user[filepath] = SumoData(filepath, timestamp, account_id, principal_id)
                counter += 1
            offset += limit
            response = await loop.run_in_executor(None, sumo.search_job_messages, search_job, limit, offset)
    logging.info(f"Total Number of Logs Processed: {counter}")
    num_log_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_sumo_query_number_logs_{version}", f"{job_description} - Number of logs analyzed per airflow run", []
    )
    num_log_gauge.set(counter)
    logging.info(f"Total Number of Unique filepaths: {len(filepath_to_timestamp_user.items())}")

    end_time = time.time()
    logging.info(f"Execution time of Sumo Query: {(end_time - start_time) / 60} min.")
    sumo_log_timer_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_sumo_query_execution_time_{version}",
        f"{job_description} - This metric tracks how long (in seconds) it takes to query the past 15 minutes of sumo logs and store it into memory",
        []
    )
    sumo_log_timer_gauge.set(end_time - start_time)
    return filepath_to_timestamp_user


def get_db_connection(conn_name):
    conn_info = BaseHook.get_connection(conn_name)
    server = conn_info.host
    user = conn_info.login
    password = conn_info.password
    database = conn_info.schema
    return pymssql.connect(server=server, user=user, password=password, database=database)


def _store_reds_last_accessed(values) -> None:
    """Returns REDS feed definitions from ProvDB."""
    conn = get_db_connection(connection_string_provisioning)
    logging.info(f"Storing {len(values)} records into Provisioning Database")
    redsfeed.RedsFeed.log_feed_access_batch(conn, values)
    conn.close()


def _store_exposure_last_accessed(values) -> None:
    """Returns Exposure feed definitions from SchedReporting."""
    conn = get_db_connection(connection_string_schedreporting)
    logging.info(f"Storing {len(values)} records into SchedReporting Database")
    exposurefeed.ExposureFeed.log_feed_access_batch(conn, values)
    conn.close()


def list_reds_regex_paths_feedid() -> List[Tuple[str, str, str]]:
    """Lists all S3 filepaths that represent REDS feeds (including normal and aggregated feeds) to be inserted into
    the Trie
    """
    conn = get_db_connection(connection_string_provisioning)
    feeds = redsfeed.RedsFeed.all(conn)
    conn.close()
    logging.info(f'Parsing {len(feeds)} REDS feeds...')
    filepaths = [(feed.destination_path_regex, feed.destination_path_regex_always_unconcatenated, feed.feed_id) for feed in feeds]
    return filepaths


def list_exposure_regex_paths_feedid() -> List[Tuple[str, str]]:
    """Lists all S3 filepaths that represent Exposure feeds to be inserted into the Trie.
    """
    conn = get_db_connection(connection_string_schedreporting)
    feeds = exposurefeed.ExposureFeed.all(conn)
    conn.close()
    logging.info(f'Parsing {len(feeds)} Exposure feeds...')
    # Not including thetradedesk-useast-partner-datafeed into the Trie because all feeds in this bucket have feed_id
    # in filepath.
    s3_feeds = [feed for feed in feeds if feed.bucket != 'thetradedesk-useast-partner-datafeed']
    logging.info(f'Found {len(s3_feeds)} exposure feeds that is not in thetradedesk-useast-partner-datafeed bucket')

    filepaths = [(feed.destination_path_regex, feed.feed_id) for feed in s3_feeds]
    return filepaths


def list_reds_feedids() -> List[str]:
    """Lists all S3 feed_ids that represent REDS feeds (including normal and aggregated feeds) to be inserted into
    the Trie
    """
    conn = get_db_connection(connection_string_provisioning)
    logging.info('Grabbing REDS FeedIds from Provisioning Database...')
    reds_feed_ids = redsfeed.RedsFeed.get_reds_feed_ids(conn)
    conn.close()
    return reds_feed_ids


def list_exposure_feedids() -> List[str]:
    """Lists all S3 filepaths that represent REDS feeds (including normal and aggregated feeds) to be inserted into
    the Trie
    """
    conn = get_db_connection(connection_string_schedreporting)
    logging.info('Grabbing Exposure FeedIds from SchedReporting Database...')
    exposure_feed_ids = exposurefeed.ExposureFeed.get_exposure_feed_ids(conn)
    conn.close()
    return exposure_feed_ids


async def build_trie() -> FilePathTrie:
    logging.info("Started function build_trie()")
    start_time = time.time()

    trie = FilePathTrie()
    logging.info("Started storing reds feeds in Trie")
    for destination_path_regex, unconcatenated_destination_path_regex, feed_id in list_reds_regex_paths_feedid():
        if destination_path_regex != unconcatenated_destination_path_regex:
            trie.insert(destination_path_regex, feed_id)
        trie.insert(unconcatenated_destination_path_regex, feed_id)
    logging.info("Started storing exposure feeds in Trie")
    for destination_path_regex, feed_id in list_exposure_regex_paths_feedid():
        trie.insert(destination_path_regex, feed_id)

    logging.info(f"Stored {len(list_reds_regex_paths_feedid())} reds feeds into the Trie.")
    logging.info(f"Stored {len(list_exposure_regex_paths_feedid())} exposure feeds into the Trie.")
    end_time = time.time()
    build_trie_timer_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_build_trie_execution_time_{version}",
        f"{job_description} - This metric tracks how long (in seconds) it takes to find build a trie out of the feeds in the DB", []
    )
    build_trie_timer_gauge.set(end_time - start_time)
    logging.info(f"Execution time of Build Trie: {(end_time - start_time) / 60} min.")
    return trie


def check_regex_match(filename):
    pattern = re.match(aggregatedfilepattern1, filename)
    # file name follows the format: <date><hour><feedtype><version><feedid>0
    if pattern:
        return pattern.group(5)
    # file name follows the format: <feedtype>_<feedid>_<processedtime>_<hash>
    elif pattern := re.match(aggregatedfilepattern2, filename):
        return pattern.group(2)
    # file name ends in the format: <feedid>_<hash>
    elif pattern := re.search(aggregatedfilepattern3, filename):
        return pattern.group(1)
    return None


def check_s3_metadata(filepath):
    try:
        s3 = boto3.client('s3')
        logging.info(f"Attempting to search for metadata in location {filepath}")
        bucket_name, key = filepath.split('/', 1)
        response = s3.head_object(Bucket=bucket_name, Key=key)
        if 'Metadata' in response:
            metadata = response['Metadata']
            feed_id = metadata.get('feed-id')
            if feed_id:
                logging.info(f"Found feedid: {feed_id} from metadata!")
                return feed_id
    except Exception as e:
        logging.error(f"Unable to find metadata due to the following exception: {e}")
    return None


def validate_feedid_and_update_dict(feed_id, feed_id_to_data, sumo_data, db_feed_ids):
    feed_id = int(feed_id)
    if feed_id not in db_feed_ids:
        feed_id = int(check_s3_metadata(sumo_data.filepath))
        sumo_data.feed_id = feed_id
    if feed_id not in feed_id_to_data or sumo_data.last_accessed_datetime > feed_id_to_data[feed_id].last_accessed_datetime:
        logging.info(f"Found filepath: {sumo_data.filepath} to feedid: {feed_id} match to store into dictionary")
        sumo_data.set_feed_id(feed_id)
        feed_id_to_data[feed_id] = sumo_data
    return feed_id_to_data


def find_feed_id_from_filepath(trie: FilePathTrie, sumo_logs: Dict[str, SumoData]) -> Dict[int, SumoData]:
    logging.info("Started function find_feedid_from_filepath()")
    start_time = time.time()

    matches = 0
    fail_matches = 0
    feed_id_to_data: Dict[int, SumoData] = {}
    logging.info("Starting filepath comparison")
    db_feed_ids = list_reds_feedids() + list_exposure_feedids()

    for filepath, sumo_data in sumo_logs.items():
        filename = filepath.split('/')[-1]
        feed_id = check_regex_match(filename)
        if feed_id is not None:
            matches += 1
            feed_id_to_data = validate_feedid_and_update_dict(feed_id, feed_id_to_data, sumo_data, db_feed_ids)
        elif (feed_id := trie.search(filepath)) is not None:
            matches += 1
            feed_id_to_data = validate_feedid_and_update_dict(feed_id, feed_id_to_data, sumo_data, db_feed_ids)
        elif (feed_id := check_s3_metadata(filepath)) is not None:
            matches += 1
            feed_id_to_data = validate_feedid_and_update_dict(feed_id, feed_id_to_data, sumo_data, db_feed_ids)
        else:
            fail_matches += 1
            logging.error(f"Could not feed_id using regex, trie, or S3 for filepath: {filepath}")

    logging.info(f"Matches: {matches}")
    logging.info(f"FeedIds Found: {feed_id_to_data.keys()}")
    logging.info(f"Failed Matches: {fail_matches}")
    logging.info(f"Duplicate Counter: {matches - len(feed_id_to_data)}")
    logging.info(f"Success Rate: {float(matches / len(sumo_logs)) * 100}%")
    end_time = time.time()
    num_unique_feed_ids_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_num_unique_feed_ids_{version}", f"{job_description} - Number of unique feed ids found per airflow run", []
    )
    num_unique_feed_ids_gauge.set(len(feed_id_to_data.keys()))
    filepath_compare_success_rate_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_filepath_compare_success_rate_{version}",
        f"{job_description} - Success Rate when finding feed ids from filepaths", []
    )
    filepath_compare_success_rate_gauge.set(float(matches / len(sumo_logs)) * 100)
    filepath_compare_timer_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_filepath_compare_execution_time_{version}",
        f"{job_description} - This metric tracks how long (in seconds) it takes to find feed ids from filepaths", []
    )
    filepath_compare_timer_gauge.set(end_time - start_time)
    logging.info(f"Execution time of Filepath/FeedId Comparison: {(end_time - start_time) / 60} min.")
    return feed_id_to_data


def update_last_accessed_table(feedid_to_data: Dict[int, SumoData]):
    logging.info("Started update_last_accessed_table()")
    start_time = time.time()
    failures = 0
    db_reds_info = []
    db_exposure_info = []
    reds_feed_ids = list_reds_feedids()
    exposure_feed_ids = list_exposure_feedids()
    for feed_id, data in feedid_to_data.items():
        if feed_id in reds_feed_ids:
            db_reds_info.append((feed_id, data.last_accessed_datetime, data.account_id, data.principal_id))
        elif feed_id in exposure_feed_ids:
            db_exposure_info.append((feed_id, data.last_accessed_datetime, data.account_id, data.principal_id))
        else:
            failures += 1
            logging.error(f"Attempted to insert feed_id({feed_id}) that does not exist")
    _store_reds_last_accessed(db_reds_info)
    _store_exposure_last_accessed(db_exposure_info)
    end_time = time.time()
    logging.info(f"Execution time of Storing Data into DB: {(end_time - start_time) / 60} min.")
    logging.info(f"Store into DB Success Rate: {(len(feedid_to_data) - failures) / len(feedid_to_data) * 100}%")
    store_db_success_rate_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_store_db_success_rate_{version}", f"{job_description} - Success Rate when storing into the DB", []
    )
    store_db_success_rate_gauge.set((len(feedid_to_data) - failures) / len(feedid_to_data) * 100)
    store_db_timer_gauge: Gauge = get_or_register_gauge(
        job_name, f"{job_name}_store_db_execution_time_{version}",
        f"{job_description} - This metric tracks how long (in seconds) it takes store data into the DBs", []
    )
    store_db_timer_gauge.set(end_time - start_time)


async def async_update_last_accessed_task(execution_datetime):
    trie, sumo_logs = await asyncio.gather(build_trie(), store_sumo_logs(execution_datetime))
    feedid_to_datetime_user = find_feed_id_from_filepath(trie, sumo_logs)
    update_last_accessed_table(feedid_to_datetime_user)


def update_last_accessed_task(**kwargs):
    execution_datetime = kwargs['logical_date']
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_update_last_accessed_task(execution_datetime))
    push_all(job_name)


# Task which includes Querying Sumo Logs, Building Trie, Comparing Filepaths/FeedIDs, Storing in DB
update_last_accessed_task = OpTask(
    op=PythonOperator(
        task_id='update_last_accessed_task',
        python_callable=update_last_accessed_task,
        provide_context=True,
        queue=Workers.k8s.queue,
        pool=Workers.k8s.pool,
        executor_config=TaskServicePodResources.large().as_executor_config(),
        dag=adag,
    )
)

# Define task dependencies
dag >> update_last_accessed_task
