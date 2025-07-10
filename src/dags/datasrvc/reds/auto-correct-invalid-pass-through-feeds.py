import json
import random
from airflow import DAG
from datetime import timedelta
from datetime import datetime
import logging

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.ttdslack import dag_post_to_slack_callback
from ttd.ttdslack import get_slack_client

__doc__ = """
This Airflow involves identifying and automatically correcting invalid pass-through feeds.
The corrections are logged for reference, and alerts are sent out to ensure prompt awareness
of any adjustments made to maintain data accuracy
"""

dag_name = "auto-correct-invalid-pass-through-feeds"
start_date = datetime(2024, 8, 15, hour=0)
execution_interval = timedelta(minutes=60 * 12)
provisioning_conn_id = "provdb-readonly"
db_name = "Provisioning"
alarm_slack_datasrvc_channel = "#scrum-data-services-alarms"
alarm_slack_reds_channel = "#ftr-reds"

get_invalid_pass_through_feeds_task_id = "get_invalid_pass_through_feeds"
feeds_allow_passthrough_list = {22626, 22881, 22882, 22883, 22884}

default_args = {
    "owner": "datasrvc",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(seconds=300),
}

dag = DAG(
    dag_name,
    default_args=default_args,
    description="Identify and set invalid pass-though feeds to be scrubbed",
    schedule_interval=execution_interval,
    start_date=start_date,
    catchup=False,
    tags=["DATASRVC", "REDS", "SQL"],
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_name,
        step_name="parent dagrun",
        slack_channel="#scrum-data-services-alarms",
    ),
    max_active_runs=2,
    doc_md=__doc__,
)


class Feed:

    def __init__(self, feed_id, feed_type, partner_id, provider_id, vendor_id):
        self.feed_id = feed_id
        self.feed_type = feed_type
        self.partner_id = partner_id
        self.provider_id = provider_id
        self.vendor_id = vendor_id


def _get_invalid_pass_through_feeds(**kwargs):
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id, schema="Provisioning")
    connection = sql_hook.get_conn()
    connection.autocommit(True)
    cursor = connection.cursor()

    sql = """
        SELECT f.FeedId, ft.FeedTypeName, f.PartnerId, pvm.ProviderId, pvm.VendorId
        FROM reds.feed f
        INNER JOIN reds.FeedType ft on ft.FeedTypeId = f.FeedTypeId
        LEFT JOIN dbo.PartnerGdprVendorId pvm ON f.partnerId = pvm.PartnerId
        LEFT JOIN gdpr.GVLProviderVendor gvl ON pvm.ProviderId = gvl.ProviderId
            AND pvm.VendorId = gvl.VendorId
        WHERE f.GdprConsentHandlingTypeId = reds.fn_Enum_GdprConsentHandlingType_IncludeRow()
            AND f.FeedStatusId = reds.fn_Enum_FeedStatus_Active()
            AND f.FeedTypeId <> reds.fn_Enum_FeedType_GdprConsent()
            AND (pvm.vendorId IS NULL OR gvl.DeletedDateUtc IS NOT NULL)"""

    cursor.execute(sql)
    row = cursor.fetchone()
    feed_set = set()
    feeds = []
    while row:
        feed_id = row[0]
        if feed_id in feed_set:
            raise Exception(f"FeedId {feed_id} was found more than once.")
        feed_set.add(feed_id)
        feeds.append(Feed(row[0], row[1], row[2], row[3], row[4]))
        row = cursor.fetchone()

    cursor = cursor.close()
    connection.close()

    serialized_feeds = [json.dumps(feed_instance.__dict__) for feed_instance in feeds]
    kwargs["ti"].xcom_push(key="feeds", value=serialized_feeds)

    logging.info(f"Invalid passthrough feeds: {serialized_feeds}")


def _set_invalid_feed_to_scrubbed(**kwargs):
    serialized_feeds = kwargs["ti"].xcom_pull(task_ids=get_invalid_pass_through_feeds_task_id, key="feeds")
    if serialized_feeds is None or not serialized_feeds:
        return

    feeds = [Feed(**json.loads(serialized_feed)) for serialized_feed in serialized_feeds]
    if not feeds:
        return

    feeds_to_scrub = [feed for feed in feeds if feed.feed_id not in feeds_allow_passthrough_list]
    excluded_feeds = [feed for feed in feeds if feed.feed_id in feeds_allow_passthrough_list]

    if excluded_feeds:
        excluded_feed_ids = ",".join(str(feed.feed_id) for feed in excluded_feeds)
        logging.info(f"Excluded feeds due to allow passthrough list: {excluded_feed_ids}")
    else:
        logging.info("No feeds excluded from scrubbing based on the allow passthrough list.")

    if not feeds_to_scrub:
        logging.info("No feeds to scrub after applying the allow passthrough list.")
        return

    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id, schema="Provisioning")
    connection = sql_hook.get_conn()
    connection.autocommit(True)
    cursor = connection.cursor()

    feed_change_event_type_id = 8  # JobModification
    user_id = "zco760j"
    run_number = random.randint(100000, 999999)
    comment = f"{run_number}-{dag_name}"

    invalid_feed_ids = ",".join(str(feed.feed_id) for feed in feeds_to_scrub)
    logging.info(f"run_number: {run_number}, invalid_feed_ids: {invalid_feed_ids}")

    sql = (
        f"EXEC reds.prc_UpdateInvalidPassThroughFeedsToBeScrubbed @InvalidFeedIds = '{invalid_feed_ids}', " +
        f"@FeedChangeEventTypeId = {feed_change_event_type_id}, " + f"@UserId = '{user_id}', @Comment = '{comment}'"
    )

    logging.info(f"\n{sql}\n")

    cursor.execute(sql)

    cursor = cursor.close()
    connection.close()

    serialized_feeds = [json.dumps(feed_instance.__dict__) for feed_instance in feeds_to_scrub]
    kwargs["ti"].xcom_push(key="invalidFeedsAfterFiltering", value=serialized_feeds)

    logging.info(f"Invalid passthrough feeds after applying the allow list: {serialized_feeds}")


def format_feeds_to_tsv(feeds):
    header = ["feed_id", "feed_type", "partner_id", "provider_id", "vendor_id"]
    header_string = "\t".join(header)

    feeds_string = ""
    for feed_instance in feeds:
        feed_values = [str(getattr(feed_instance, field)) for field in header]
        feeds_string += "\t".join(feed_values) + "\n"

    return f"{header_string}\n{feeds_string}"


def _send_slack_alert(**kwargs):
    serialized_feeds = kwargs["ti"].xcom_pull(task_ids=get_invalid_pass_through_feeds_task_id, key="invalidFeedsAfterFiltering")
    if serialized_feeds is None or not serialized_feeds:
        logging.info("No invalid feeds need to be alerted.")
        return

    feeds = [Feed(**json.loads(serialized_feed)) for serialized_feed in serialized_feeds]
    if not feeds:
        return

    logging.info("send_slack_alert")
    invalid_feed_ids = ",".join(str(feed.feed_id) for feed in feeds)
    logging.info(f"invalid_feed_ids: {invalid_feed_ids}")

    formatted_tsv_string = format_feeds_to_tsv(feeds)
    tsv_text = f"List of invalid GDPR pass-through feeds set to be scrubbed automatically:\n{formatted_tsv_string}."
    logging.info(f"{tsv_text}")

    get_slack_client().chat_postMessage(
        channel=alarm_slack_datasrvc_channel,
        text=tsv_text,
    )

    get_slack_client().chat_postMessage(
        channel=alarm_slack_reds_channel,
        text=tsv_text,
    )


get_invalid_pass_through_feeds = PythonOperator(
    task_id=get_invalid_pass_through_feeds_task_id,
    python_callable=_get_invalid_pass_through_feeds,
    dag=dag,
    provide_context=True,
)

set_invalid_feed_to_scrubbed = PythonOperator(
    task_id="set_invalid_feed_to_scrubbed",
    python_callable=_set_invalid_feed_to_scrubbed,
    dag=dag,
    provide_context=True,
)

send_slack_alert = PythonOperator(
    task_id="send_slack_alert",
    python_callable=_send_slack_alert,
    dag=dag,
    provide_context=True,
)

get_invalid_pass_through_feeds >> set_invalid_feed_to_scrubbed >> send_slack_alert
