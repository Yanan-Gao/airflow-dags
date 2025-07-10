"""
Monitoring of dbo.TaskLogTypeBlockListWithOverride table in LogWorkflowDB: that it doesn't have forgotten items
"""
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

job_name = 'taskLogTypeBlockList-check'

lwdb_conn_id = 'lwdb'

alarm_slack_channel = '#scrum-data-services-alarms'

default_args = {
    'owner': 'datasrvc',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': False,
}

# The top-level dag
dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 2 * * *",  # 2:00 every day
    run_only_latest=True,
    slack_channel='#scrum-data-services-alarms',
    slack_tags=DATASRVC.team.jira_team,
    tags=["DATASRVC", "SQL"],
    max_active_runs=1,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
)

adag = dag.airflow_dag

lwdb_counts_query = """
    SELECT DISTINCT
    l.LogTypeName,
    t.TaskName,
    tv.TaskVariantName
    FROM
        TaskLogTypeBlockListWithOverride tl
    JOIN
        LogType l ON tl.LogTypeId = l.LogTypeId
    JOIN
        Task t ON tl.TaskId = t.TaskId
    JOIN
        TaskVariant tv ON tl.TaskVariantId = tv.TaskVariantId
    WHERE
        tl.DisableAlertsEndTime < GETUTCDATE();
"""


def check_taskLogTypeBlockList():
    lwfhook = MsSqlHook(mssql_conn_id=lwdb_conn_id, schema='Logworkflow')
    lwfconn = lwfhook.get_conn()
    lwfconn.autocommit(True)

    result = lwfhook.get_records(lwdb_counts_query)

    if len(result) != 0:
        entries = ', '.join(f"(LogTypeName: {row[0]}, TaskName: {row[1]}, TaskVariantName: {row[2]})" for row in result)
        message = (
            f"There are entries in the table dbo.TaskLogTypeBlockListWithOverride for {entries} which are old. " +
            "Please clean up.  https://thetradedesk.atlassian.net/l/cp/AxAJaQz2. " +
            "Check both dbo.TaskLogTypeBlockList and dbo.TaskLogTypeBlockListOverride for old entries."
        )
        raise ValueError(message)


check_taskLogTypeBlockList = OpTask(
    op=PythonOperator(
        task_id='check_taskLogTypeBlockList',
        python_callable=check_taskLogTypeBlockList,
        dag=adag,
    )
)

dag >> check_taskLogTypeBlockList
