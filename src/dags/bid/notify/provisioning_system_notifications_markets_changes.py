from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from ttd.ttdslack import dag_post_to_slack_callback
from datetime import datetime, timedelta
import logging
import requests
import json

# Notifies markets slack channels on changes to certain provisioning tables

dag_name = 'ProvisioningSystem-Notifications-MarketsChanges'
alarm_slack_channel = '#inv-marketplace-alerts'
sql_connection_id = 'provisioning_replica'
db_name = 'Provisioning'
schedule_interval = '*/10 * * * *'
webhook = 'https://hooks.slack.com/services/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'


def choose_sql_to_run(begin_date_inclusive, end_date_exclusive):
    date_filter = ""
    if begin_date_inclusive != end_date_exclusive:
        date_filter = f" and t.MODIFIED_DATE >= '{(begin_date_inclusive.strftime('%Y-%m-%d %H:%M:%S'))}' and t.MODIFIED_DATE < '{(end_date_exclusive.strftime('%Y-%m-%d %H:%M:%S'))}'"
    tsql = """select *
from (
    select t.MODIFIED_DATE, concat('via: ', s.SupplyVendorName, '/', svp.SupplyVendorPublisherId) as Path, p.PublisherId, svp.SupplyVendorPublisherName, p.PublisherDomain, paso.Name OldPublisherState, pasn.Name NewPublisherState, svp.Comment, coalesce(t.APPLICATION_USER, t.MODIFIED_BY) Username
    from AuditLogTarget.dbo.AUDIT_LOG_TRANSACTIONS t
        inner join AuditLogTarget.dbo.AUDIT_LOG_DATA d on t.AUDIT_LOG_TRANSACTION_ID = d.AUDIT_LOG_TRANSACTION_ID
        left join provisioning.supply.SupplyVendorPublisher svp on d.KEY1 = svp.SupplyVendorPublisherTTDId
        left join provisioning.supply.publisher p on svp.PublisherId = p.PublisherId
        left join provisioning.dbo.PublisherApprovalState pasn on d.NEW_VALUE = pasn.PublisherApprovalStateId
        left join provisioning.dbo.PublisherApprovalState paso on d.OLD_VALUE = paso.PublisherApprovalStateId
    left join dbo.SupplyVendor s on svp.SupplyVendorId = s.SupplyVendorId
    where t.TABLE_SCHEMA = 'supply'
        and t.TABLE_NAME = 'SupplyVendorPublisher'
        and d.COL_NAME = 'PublisherApprovalStateId'
        and t.AUDIT_ACTION_ID = 1{}
union all
    select t.MODIFIED_DATE, 'whole publisher' as Path, p.PublisherId, p.PublisherName, p.PublisherDomain, paso.Name OldPublisherState, pasn.Name NewPublisherState, p.Comment, coalesce(t.APPLICATION_USER, t.MODIFIED_BY) Username
    from AuditLogTarget.dbo.AUDIT_LOG_TRANSACTIONS t
        inner join AuditLogTarget.dbo.AUDIT_LOG_DATA d on t.AUDIT_LOG_TRANSACTION_ID = d.AUDIT_LOG_TRANSACTION_ID
        left join provisioning.supply.publisher p on d.KEY1 = p.PublisherId
        left join provisioning.dbo.PublisherApprovalState pasn on d.NEW_VALUE = pasn.PublisherApprovalStateId
        left join provisioning.dbo.PublisherApprovalState paso on d.OLD_VALUE = paso.PublisherApprovalStateId
    where t.TABLE_SCHEMA = 'supply'
        and t.TABLE_NAME = 'publisher'
        and d.COL_NAME = 'PublisherApprovalStateId'
        and t.AUDIT_ACTION_ID = 1{}
) d
order by MODIFIED_DATE
""".format(date_filter, date_filter)

    return tsql


def post_row_to_slack(row):
    modified_date, path, publisher_id, publisher_name, publisher_domain, old_state, new_state, comment, user = row
    if "OPS\\" in user:
        cleaned_user = user.split("\\")[1]
    elif r"@thetradedesk.com" in user:
        cleaned_user = user.split(r"@")[0]
    else:
        cleaned_user = user
    send_slack_alert(
        f"`{cleaned_user}` modified publisher `{publisher_name} - {publisher_domain} ({publisher_id})` {path} at {(modified_date.strftime('%Y-%m-%d, %H:%M'))} from `{old_state}` to `{new_state}` for reason\n```{comment}```"
    )


def send_slack_alert(message):
    requests.post(webhook, data=json.dumps({"text": message}), headers={'Content-Type': 'application/json'})


def notifications_markets_changes(**kwargs):
    window_start = kwargs['data_interval_start'] - timedelta(minutes=15)
    window_end = kwargs['data_interval_end'] - timedelta(minutes=15)
    logging.info(f"Running for range from {window_start} to {window_end}")
    sql_hook = MsSqlHook(mssql_conn_id=sql_connection_id, schema=db_name)
    connection = sql_hook.get_conn()
    connection.autocommit(True)
    cursor = connection.cursor()
    sql = choose_sql_to_run(window_start, window_end)
    logging.info(f"Executing the following statements:\n{sql}")
    cursor.execute(sql)
    row = cursor.fetchone()
    row_counts = 0
    while row:
        if row_counts > 10:
            logging.info(f"skipping row: {row}")
        else:
            logging.info(f"row: {row}")
            post_row_to_slack(row)
        row = cursor.fetchone()
        row_counts += 1
    if row_counts > 10:
        send_slack_alert(f"{row_counts - 10} further changes were skipped to avoid spamming channel")
    cursor.close()
    connection.close()


create_notification_dag = DAG(
    dag_name,
    schedule_interval=schedule_interval,
    tags=['Notify', 'MSSQL'],
    default_args={
        'owner': 'BID',
        'start_date': datetime(2022, 9, 22),
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
    },
    max_active_runs=1,  # we only want 1 run at a time. Sproc shouldn't run multiple times concurrently
    catchup=False,  # Just execute the latest run. It's fine to skip old runs
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_name,
        step_name='Notifications_MarketsChanges',
        slack_channel='#dev-dba-airflow-jobs',
        tsg='https://atlassian.thetradedesk.com/confluence/display/EN/DBJobs',
        slack_tags='Greg.Robertson@thetradedesk.com'
    ),
)

create_notification_task = PythonOperator(
    task_id="Notifications_MarketsChanges",
    python_callable=notifications_markets_changes,
    dag=create_notification_dag,
    provide_context=True,
)
