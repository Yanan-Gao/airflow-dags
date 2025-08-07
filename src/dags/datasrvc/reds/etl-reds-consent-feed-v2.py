"""
Generate REDS consent feeds from Snowflake
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from multiprocessing.pool import ThreadPool
import multiprocessing as mp

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdslack import dag_post_to_slack_callback
from datetime import datetime, timedelta
import hashlib

# Enable testing mode: Use 'dev-' prefix for staging, and write consent files to the test bucket with a job-specific prefix in the path
testing = False

# Dry run: Print logs only without executing file cleanup, writing files, or running actual queries on Snowflake
dry_run = False


def choose(prod, test):
    return test if testing else prod


testing_mode_prefix = choose(prod='', test='dev-')
job_name = f'{testing_mode_prefix}etl-reds-consent-feed-v2'
testing_mode_s3_path_prefix = choose(prod='', test=f'{job_name}/')

staging_bucket = 'thetradedesk-useast-partner-datafeed-airflow-stage'
reds_bucket = choose(prod='thetradedesk-useast-partner-datafeed', test='thetradedesk-useast-partner-datafeed-test2')

provisioning_conn_id = 'provdb-readonly'
snowflake_conn_id = 'snowflake'

snowflake_stage = '@Snowflake_Airflow_Stage'
snowflake_schema = 'REDS'
snowflake_warehouse = 'TTD_AIRFLOW'
snowflake_database = 'THETRADEDESK'

execution_interval = timedelta(minutes=60)

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': False,
}

dag = DAG(
    dag_id=job_name,
    default_args=default_args,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
    start_date=datetime(2024, 8, 27, hour=4),
    catchup=True,
    tags=["DATASRVC", "SQL", "Snowflake"],
    schedule_interval=execution_interval,
    max_active_runs=2,
)


class Feed:

    def __init__(self, feed_id, destination_location, version, partner_id, advertiser_id):
        self.feed_id = feed_id
        self.destination_location = destination_location
        self.version = version
        self.partner_id = partner_id
        self.advertiser_id = advertiser_id


def get_consent_feeds():
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id, schema='Provisioning')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sql = """
        select f.FeedId, DestinationLocation, [Version], PartnerId, AdvertiserId
        from reds.Feed f
        left join reds.FeedAdvertiser fa on fa.FeedId = f.FeedId
        where FeedTypeId = reds.fn_Enum_FeedType_GdprConsent()
            and FeedDestinationTypeId = reds.fn_Enum_FeedDestinationType_S3()
            and FeedStatusId = reds.fn_Enum_FeedStatus_Active()"""
    cursor.execute(sql)
    row = cursor.fetchone()
    feed_id_set = set()
    feeds = []
    while row:
        feed_id = row[0]
        if feed_id in feed_id_set:
            raise Exception(f'FeedId {feed_id} was found more than once.')
        feed_id_set.add(feed_id)
        feeds.append(Feed(row[0], row[1], row[2], row[3], row[4]))
        row = cursor.fetchone()
    return feeds


def get_partners_with_consent_feeds():
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id, schema='Provisioning')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sql = """
        select distinct PartnerId
        from reds.Feed f
        where FeedTypeId = reds.fn_Enum_FeedType_GdprConsent()
            and FeedDestinationTypeId = reds.fn_Enum_FeedDestinationType_S3()
            and FeedStatusId = reds.fn_Enum_FeedStatus_Active()"""
    cursor.execute(sql)
    row = cursor.fetchone()
    partner_ids = []
    while row:
        partner_id = "'" + row[0] + "'"
        partner_ids.append(partner_id)
        row = cursor.fetchone()
    return ','.join(partner_ids)


def get_organized_keys(files):
    if files is None:
        return {}
    organized_keys: dict[str, dict[str, list[str]]] = {}
    for f in files:
        parts = f.split('/')
        # parts[0] is the timestamp of the dagrun
        partner_id = parts[1]
        advertiser_id = parts[2]
        if partner_id not in organized_keys:
            organized_keys[partner_id] = {}
        if advertiser_id not in organized_keys[partner_id]:
            organized_keys[partner_id][advertiser_id] = []
        organized_keys[partner_id][advertiser_id].append(f)
    return organized_keys


def partially_resolve_destination_location(f, timestamp):
    result = f.destination_location
    if "<advertiserid>" in f.destination_location:
        if f.advertiser_id is None:
            raise Exception(f'FeedId {f.feed_id} has the pattern <advertiserid>, but does not have exactly one advertiser')
        else:
            result = result.replace('<advertiserid>', f.advertiser_id)
    # <hash> <date> and <hour> will be replaced later when we know which file we are copying
    return result \
        .replace('<partnerid>', f.partner_id) \
        .replace('<feedid>', str(f.feed_id)) \
        .replace('<version>', str(f.version)) \
        .replace('<feedtype>', 'consent') \
        .replace('<processedtime>', timestamp[0:4] + '-' + timestamp[4:6] + '-' + timestamp[6:])


def get_date_hour_elements(key):
    parts = key.split('/')
    if not parts[-3].startswith('date='):
        raise Exception('date element is not the third last element')
    if not parts[-2].startswith('hour='):
        raise Exception('hour elemement is not the second last element')
    return parts[-3], parts[-2]


def copy_file(source_key, destination_location, s3):
    if '<hash>' not in destination_location:
        raise Exception('destination location does not contain <hash> element')
    destination_location = destination_location.replace('<hash>', hashlib.md5(source_key.encode()).hexdigest())
    date, hour = get_date_hour_elements(source_key)
    destination_location = destination_location \
        .replace('date=<date>', date) \
        .replace('hour=<hour>', hour)
    i = destination_location.index('/')
    target_bucket = destination_location[:i].replace('<bucket>', reds_bucket)
    target_key = testing_mode_s3_path_prefix + destination_location[i + 1:]
    print("source key=" + source_key + ", target key=" + target_key)
    print("staging_bucket=" + staging_bucket + ", target_bucket=" + target_bucket)
    if dry_run:
        print("Dry run mode is enabled. Skipping.")
    else:
        s3.copy_file(source_key, staging_bucket, target_key, target_bucket)


def copy_to_datafeed_bucket_multithreaded(timestamp):
    # First get all the available files to copy from s3 and organize them
    # partner dictionary -> advertiser dictionary -> list of keys
    prefix = f'{testing_mode_prefix}{timestamp}'
    print(f"prefix={staging_bucket}/{prefix}")
    s3 = AwsCloudStorage(conn_id='aws_default')
    organized_keys = get_organized_keys(s3.list_keys(prefix, staging_bucket))

    # Then for each feed, copy the necessary files
    feeds = get_consent_feeds()
    print("Got feeds")
    pool = ThreadPool(mp.cpu_count())
    for feed in feeds:
        print("processing feed " + str(feed.feed_id))
        pool.apply_async(copy_to_datafeed_bucket, args=(feed, ), kwds={"prefix": timestamp, "keys": organized_keys, "client": s3})
    pool.close()
    pool.join()


def copy_to_datafeed_bucket(f, prefix, keys, client):
    # First get all the available files to copy from s3 and organize them
    # partner dictionary -> advertiser dictionary -> list of keys

    # Then for each feed, copy the necessary files
    print("Parsing consent feeds")
    destination_location = partially_resolve_destination_location(f, prefix)
    advertiser_dict = keys.get(f.partner_id, {})
    print("dest loc is " + destination_location)
    if f.advertiser_id is None:  # Partner level feed => copy everything
        print("partner level feed. Copy all")
        for s3_keys in advertiser_dict.values():
            for key in s3_keys:
                copy_file(key, destination_location, client)
    else:  # advertiser level feed => copy the only relevant advertiser
        print("advertiser level feed")
        for key in advertiser_dict.get(f.advertiser_id, []):
            copy_file(key, destination_location, client)


def clean_staging_bucket(prefix):
    prefix = f'{testing_mode_prefix}{prefix}'
    s3 = AwsCloudStorage(conn_id='aws_default')
    files = s3.list_keys(prefix, staging_bucket)
    if files is not None:
        print(f"clean up {len(files)} keys from staging bucket {staging_bucket}, prefix {prefix}")
        if dry_run:
            print("Dry run mode is enabled. Skipping.")
        else:
            s3.remove_objects_by_keys(staging_bucket, files)


def get_snowflake_query(**kwargs):
    timestamp = kwargs['timestamp']
    last_run_time = kwargs['logical_date'].replace(tzinfo=None)
    current_run_time = last_run_time + execution_interval
    partner_ids = get_partners_with_consent_feeds()

    # If you insert/delete commands into this array of queries, you must also update the Snowflake operator sql argument
    query = [
        f"use warehouse {snowflake_warehouse};",
        f"set CURRENT_RUN_TIME = TO_TIMESTAMP_NTZ('{current_run_time.strftime('%Y%m%dT%H%M%S')}', 'yyyymmddThh24miss');",
        f"set LAST_RUN_TIME = TO_TIMESTAMP_NTZ('{last_run_time.strftime('%Y%m%dT%H%M%S')}', 'yyyymmddThh24miss');",
        "set LOOKBACK_MINUTES = 240;",
        "set EARLIEST_LOOKBACK = TO_TIMESTAMP_NTZ(DATEADD(minutes, -$LOOKBACK_MINUTES, $LAST_RUN_TIME));   -- Earliest lookback is specified relative to prior to last run time",
        f"""
        copy into {snowflake_stage}/{testing_mode_prefix}{timestamp}/
        from
        (
            select
                bf.LogEntryTime,    -- Impression (BidFeedback) LogEntryTime
                gc.PartnerId,
                gc.AdvertiserId,
                gc.BidRequestId,
                gc.ConsentString as ConsentString,
                gc.ConsentStandard,
                gc.PartnerHasConsent::int as PartnerHasGdprConsent
            from Reds.BidFeedback bf
            inner join reds.BidRequestGdprConsent gc on
            (
                gc.PartnerId = bf.PartnerId
                and gc.AdvertiserId = bf.AdvertiserId
                and gc.BidRequestId = bf.BidRequestId
            )
            where
                bf.IsGdprApplicable = 1
                and bf.LoadTime >= $EARLIEST_LOOKBACK
                and gc.LoadTime >= $EARLIEST_LOOKBACK
                and gc.LoadTime < $CURRENT_RUN_TIME
                and bf.LoadTime < $CURRENT_RUN_TIME
                and (gc.LoadTime >= $LAST_RUN_TIME OR bf.LoadTime >= $LAST_RUN_TIME)
                and gc.PartnerId in ({partner_ids})
                and bf.PartnerId in ({partner_ids})
            order by 2,3,1
        )
        partition by (PartnerId || '/' || AdvertiserId || '/date=' || to_varchar(LogEntryTime, 'YYYY-MM-DD') || '/hour=' || to_varchar(date_part(hour, LogEntryTime)))
        FILE_FORMAT =
        (
            TYPE = CSV
            COMPRESSION = GZIP
            FIELD_OPTIONALLY_ENCLOSED_BY = NONE
            EMPTY_FIELD_AS_NULL = FALSE
            FIELD_DELIMITER = '\\t')
        HEADER = TRUE
        MAX_FILE_SIZE = 256000000  --256 MB
        ;
        """
    ]
    for i, command in enumerate(query):
        if dry_run:
            print(f'Dry run q{i}:')
            print(command)
        kwargs['ti'].xcom_push(key=f'q{i}', value=command)


# Define all operators here

initial_staging_bucket_clean = PythonOperator(
    task_id='initial_staging_bucket_clean',
    python_callable=clean_staging_bucket,
    dag=dag,
    op_kwargs={'prefix': '{{ ts_nodash }}'},
)

generate_snowflake_query = PythonOperator(
    task_id='generate_snowflake_query',
    python_callable=get_snowflake_query,
    dag=dag,
    op_kwargs={'timestamp': '{{ ts_nodash }}'},
    provide_context=True,
)

snowflake_export = EmptyOperator(
    task_id='snowflake_export_dry_run', dag=dag
) if dry_run else SnowflakeOperator(
    task_id='snowflake_export',
    dag=dag,
    snowflake_conn_id=snowflake_conn_id,
    sql=[
        '{{ ti.xcom_pull(key="q0") }}', '{{ ti.xcom_pull(key="q1") }}', '{{ ti.xcom_pull(key="q2") }}', '{{ ti.xcom_pull(key="q3") }}',
        '{{ ti.xcom_pull(key="q4") }}', '{{ ti.xcom_pull(key="q5") }}'
    ],
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    retries=0,  # no retry here because we don't want to generate different files containing the same log lines.
)

copy_to_reds_bucket = PythonOperator(
    task_id='copy_to_reds_bucket',
    python_callable=copy_to_datafeed_bucket_multithreaded,
    dag=dag,
    op_kwargs={'timestamp': '{{ ts_nodash }}'}
)

initial_staging_bucket_clean >> generate_snowflake_query >> snowflake_export >> copy_to_reds_bucket
