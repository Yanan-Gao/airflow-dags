"""
Implement Snowflake gating in LWF
"""
from datetime import datetime, timedelta

import jinja2
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from ttd.eldorado.base import TtdDag
from ttd.ttdslack import dag_post_to_slack_callback, get_slack_client
from ttd.slack.slack_groups import DATASRVC

from ttd.tasks.op import OpTask

job_name = 'snowflake-cleanse-counts-and-lwdb-gate'
lwdb_conn_id = 'lwdb'
snowflake_conn_id = 'snowflake'
snowflake_schema = 'REDS'
snowflake_warehouse = 'TTD_AIRFLOW'
snowflake_database = 'THETRADEDESK'
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': False,
}
lwdb_temp_table_name = '#temp_snowflakecountstoupdate'
snowflake_gate_delay_alarm_threshold_in_hours = 4
alarm_slack_channel = '#scrum-data-services-alarms'
dry_run_empty_query = "SELECT 1"

# The top-level dag
dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 1),
    schedule_interval="35 * * * *",  # 35th minute of every hour
    run_only_latest=True,
    slack_channel='#scrum-data-services-alarms',
    slack_tags=DATASRVC.team.jira_team,
    tags=["DATASRVC", "SQL", "Snowflake"],
    max_active_runs=1,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
)

adag = dag.airflow_dag


def get_snowflake_query(**kwargs):
    timestamp = kwargs['timestamp']
    is_dry_run = kwargs.get('dry_run', False)
    print('dry run: ', is_dry_run)
    # We insert into the reds.FeedGatingLog table before the merge so the next run does not duplicate data incase this merge runs into a deadlock and chokes. Airflow allows rerunning singular instances
    query = [
        f"use warehouse {snowflake_warehouse};",
        f"set CURRENT_RUN_TIME = TO_TIMESTAMP_NTZ('{timestamp}', 'yyyymmddThh24miss');",
        "set LAST_RUN_TIME = (select max(LastRunTime) from reds.FeedGatingLog where FEEDCATEGORY = 'ExposureFeed');",
        "insert into reds.FeedGatingLog (FeedCategory, LastRunTime) values ('ExposureFeed', $CURRENT_RUN_TIME);",
        """
        merge into reds.SnowflakeCleanseFileStats s
        using
        (
            (select 1 as LogType, f.SourceFileS3Key as SourceFileS3Key, count(*) as RowsLoaded,to_timestamp_ntz(reds.fn_GetLogStartTimeFromCleanseFileName(f.SourceFileS3Key)) as LogStartTime, $CURRENT_RUN_TIME as LastUpdated
            from reds.BidRequestGdprConsent f
            where f.LoadTime > $LAST_RUN_TIME
            and f.LoadTime <= $CURRENT_RUN_TIME
            group by f.SourceFileS3Key order by f.SourceFileS3Key)
            union all
            (select 2 as LogType, f.SourceFileS3Key as SourceFileS3Key, count(*) as RowsLoaded,to_timestamp_ntz(reds.fn_GetLogStartTimeFromCleanseFileName(f.SourceFileS3Key)) as LogStartTime, $CURRENT_RUN_TIME as LastUpdated
            from reds.Bidfeedback f
            where f.LoadTime > $LAST_RUN_TIME
            and f.LoadTime <= $CURRENT_RUN_TIME
            group by f.SourceFileS3Key order by f.SourceFileS3Key)
            union all
            (select 3 as LogType, f.SourceFileS3Key as SourceFileS3Key, count(*) as RowsLoaded,to_timestamp_ntz(reds.fn_GetLogStartTimeFromCleanseFileName(f.SourceFileS3Key)) as LogStartTime, $CURRENT_RUN_TIME as LastUpdated
            from reds.ClickTracker f
            where f.LoadTime > $LAST_RUN_TIME
            and f.LoadTime <= $CURRENT_RUN_TIME
            group by f.SourceFileS3Key order by f.SourceFileS3Key)
            union all
            (select 4 as LogType, f.SourceFileS3Key as SourceFileS3Key, count(*) as RowsLoaded,to_timestamp_ntz(reds.fn_GetLogStartTimeFromCleanseFileName(f.SourceFileS3Key)) as LogStartTime, $CURRENT_RUN_TIME as LastUpdated
            from reds.ConversionTracker f
            where f.LoadTime > $LAST_RUN_TIME
            and f.LoadTime <= $CURRENT_RUN_TIME
            group by f.SourceFileS3Key order by f.SourceFileS3Key)
            union all
            (select 31 as LogType, f.SourceFileS3Key as SourceFileS3Key, count(*) as RowsLoaded,to_timestamp_ntz(reds.fn_GetLogStartTimeFromCleanseFileName(f.SourceFileS3Key)) as LogStartTime, $CURRENT_RUN_TIME as LastUpdated
            from reds.VideoEvent f
            where f.LoadTime > $LAST_RUN_TIME
            and f.LoadTime <= $CURRENT_RUN_TIME
            group by f.SourceFileS3Key order by f.SourceFileS3Key)
        )
        as d
        on s.LogType = d.LogType and s.SourceFileS3Key = d.SourceFileS3Key and s.LogStartTime = d.LogStartTime
        when matched then
            update set s.RowsLoaded = s.RowsLoaded + d.RowsLoaded, s.LastUpdated = d.LastUpdated
        when not matched then
            insert (SourceFileS3Key,LogStartTime,LogType,RowsLoaded,LastUpdated) values (d.SourceFileS3Key,d.LogStartTime,d.LogType,d.RowsLoaded,d.LastUpdated)
        ;
        """,
    ]
    for i, command in enumerate(query):
        print('')
        if not is_dry_run:
            kwargs['ti'].xcom_push(key=f'q{i}', value=command)
        else:
            # dummy command for SnowflakeOperator to be able to find those
            kwargs['ti'].xcom_push(key=f'q{i}', value=dry_run_empty_query)


lwdb_counts_query = jinja2.Template(
    """
    select {{ snowflake_gating_id }}, {{ task_id }}, '{{ start_time }}', '{{ end_time }}', coalesce(sum(f.SuccessfulLineCount),0) as totalCount from dbo.LogFile f where f.LogStartTime >= '{{ start_time }}' and f.LogStartTime < '{{ end_time }}' and f.LogTypeId = {{ log_type_id }} and f.CloudServiceId = dbo.fn_Enum_CloudService_AWS()
    """
)

snowflake_counts_query = jinja2.Template(
    """
    select {{ snowflake_gating_id }}, {{ task_id }}, '{{ start_time }}', '{{ end_time }}', coalesce(sum(f.RowsLoaded),0)  from thetradedesk.reds.SnowflakeCleanseFileStats f where f.LogStartTime >= '{{ start_time }}' and f.LogStartTime < '{{ end_time }}' and f.LogType = {{ log_type_id }}
    """
)

create_temp_table_if_not_already_present = jinja2.Template(
    """
    if object_id( '{{ temp_table_name }}' ) is null
        CREATE TABLE {{ temp_table_name }} (
        GatingTypeId int not null,
        TaskId int not null,
        ProcessedHour datetime2 not null
    )
    """
)

insert_into_temp_table = jinja2.Template(
    """INSERT INTO {{ temp_table_name }}(GatingTypeId, TaskId, ProcessedHour)
        VALUES
        {%- for object in object_list %}
            {{ object }}{% if not loop.last %},{% else %};{% endif %}
        {%- endfor -%}
    """
)


def get_snowflake_lwdb_counts(prefix, dry_run):
    lwfcompletedtimes = {}
    # Get the Snowflake HighWaterMark values first
    sfhook = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id, warehouse=snowflake_warehouse, database=snowflake_database, schema=snowflake_schema
    )
    sfconn = sfhook.get_conn()
    sfcursor = sfconn.cursor()
    print(f"snowflake stored database {sfconn.database}")
    print(f"snowflake stored warehouse {sfconn.warehouse}")

    # Fetch the log type + hours from LWDB where LWDB has completed more hours than Snowflake
    lwfhook = MsSqlHook(mssql_conn_id=lwdb_conn_id, schema='Logworkflow')
    lwfconn = lwfhook.get_conn()
    lwfconn.autocommit(True)
    lwfcursor = lwfconn.cursor()

    snowflakerowstoupdate = []

    print("exec dbo.prc_GetSnowflakeAndLogFileTaskHighWaterMarks")
    sql = "exec dbo.prc_GetSnowflakeAndLogFileTaskHighWaterMarks"
    lwfcursor.execute(sql)
    rec = lwfcursor.fetchone()
    while rec:
        print(str(rec[0]) + '-' + str(rec[1]) + '-' + str(rec[2]) + '-' + str(rec[3]) + '-' + str(rec[4]))
        lwfcompletedtimes[str(rec[0])] = (str(rec[1]), str(rec[2]), rec[3], rec[4])
        # We insert the existing gated values for each of the log types. This is done to handle the case when max complete SF hour is 18, but the newly completed hours are 20,21. The previous version of this
        # code used to skip over the 19th hour and not wait in such a case. We now add the dummy value to handle such a case. The sproc then skips over the 18th hour in its logic
        snowflakerowstoupdate.append((rec[0], rec[1], str(rec[4])))
        rec = lwfcursor.fetchone()

    hourstocompare = []
    # Creata a list of LogType and hour that we need to compare data for in LWDB and Snowflake
    for gateid, (taskid, logtypeid, lwfvalue, snowflakevalue) in lwfcompletedtimes.items():
        starttime = snowflakevalue

        # If the delay of the Snowflake gates is greater than the threshold, Notify #DATASRVC for potential DataPipe or Snowflake Loader issues
        delay = (lwfvalue - starttime).total_seconds() / 3600
        print('snowflake gate delay: ' + str(delay))
        if delay > snowflake_gate_delay_alarm_threshold_in_hours:
            get_slack_client().chat_postMessage(
                channel=alarm_slack_channel,
                text=f'Snowflake gate {gateid} is {delay} hours behind.',
                block=[{
                    "type": "section",
                    "text": {
                        "type":
                        "plain_text",
                        "text":
                        f"Snowflake gate {gateid} is {delay} hours behind. Check TSG & airflow logs to see count difference due to potential DataPipe or Snowflake Loader issues"
                    }
                }, {
                    "type": "section",
                    "text": {
                        "type":
                        "mrkdwn",
                        "text":
                        "<https://atlassian.thetradedesk.com/confluence/x/YEjXEQ|TSG> | <https://airflow.adsrvr.org/tree?dag_id=snowflake-lwdb-gate|TreeView>"
                    }
                }]
            )

        while (starttime <= lwfvalue):
            endtime = starttime + timedelta(hours=1)
            hourstocompare.append((gateid, taskid, logtypeid, starttime, endtime))
            print("appended...")
            starttime = endtime

    # Build queries to run against LWDB and Snowflake
    lwdbQuery = ""
    snowflakeQuery = ""
    for item in hourstocompare:
        if lwdbQuery != "":
            lwdbQuery = lwdbQuery + " union all "
        lwdbQuery = lwdbQuery + lwdb_counts_query.render(
            snowflake_gating_id=item[0], task_id=item[1], log_type_id=item[2], start_time=item[3], end_time=item[4]
        )

        if snowflakeQuery != "":
            snowflakeQuery = snowflakeQuery + " union all "
        snowflakeQuery = snowflakeQuery + snowflake_counts_query.render(
            snowflake_gating_id=item[0], task_id=item[1], log_type_id=item[2], start_time=item[3], end_time=item[4]
        )

    snowflakelwdbcomparison = {}

    print("lwdb union " + lwdbQuery)
    print("snowflake union " + snowflakeQuery)
    if (lwdbQuery != "" and snowflakeQuery != ""):
        # Get LWDB counts
        lwfcursor.execute(lwdbQuery)
        rec = lwfcursor.fetchone()
        while rec:
            snowflakelwdbcomparison[str(rec[0]) + str(rec[3])] = int(rec[4])
            rec = lwfcursor.fetchone()

        # Get Snowflake counts. If counts for an hour match, add to final list
        sfcursor.execute(snowflakeQuery)
        rec = sfcursor.fetchone()
        while rec:
            if (snowflakelwdbcomparison[str(rec[0]) + str(rec[3])] == int(rec[4])):
                print(str(rec[0]) + '-' + str(rec[1]) + '-' + str(rec[2]))
                snowflakerowstoupdate.append((rec[0], rec[1], rec[3]))
            rec = sfcursor.fetchone()

        if (snowflakerowstoupdate):
            print("creating table " + create_temp_table_if_not_already_present.render(temp_table_name=lwdb_temp_table_name))
            lwfcursor.execute(create_temp_table_if_not_already_present.render(temp_table_name=lwdb_temp_table_name))

            print(
                "inserting table " + insert_into_temp_table.render(temp_table_name=lwdb_temp_table_name, object_list=snowflakerowstoupdate)
            )
            lwfcursor.execute(insert_into_temp_table.render(temp_table_name=lwdb_temp_table_name, object_list=snowflakerowstoupdate))

            lwfcursor.execute("select * from #temp_snowflakecountstoupdate")
            rec = lwfcursor.fetchone()
            while rec:
                print(str(rec[0]) + ',' + str(rec[1]) + ',' + str(rec[2]))
                rec = lwfcursor.fetchone()

            # Call LWDB Sproc
            print("Calling the sproc")
            if not dry_run:
                sql = "exec WorkflowEngine.prc_UpdateSnowflakeGateWatermark"
                lwfcursor.execute(sql)
    lwfconn.close()
    sfconn.close()


# Define all operators here
generate_snowflake_queries_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id='generate_snowflake_queries',
        python_callable=get_snowflake_query,
        op_kwargs={'timestamp': '{{ ts_nodash }}'},
        provide_context=True,
    )
)

run_snowflake_queries_task = OpTask(
    op=SnowflakeOperator(
        dag=adag,
        task_id='run_snowflake_queries',
        snowflake_conn_id=snowflake_conn_id,
        sql=[
            '{{ ti.xcom_pull(key="q0") }}',
            '{{ ti.xcom_pull(key="q1") }}',
            '{{ ti.xcom_pull(key="q2") }}',
            '{{ ti.xcom_pull(key="q3") }}',
            '{{ ti.xcom_pull(key="q4") }}',
        ],
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        retries=0,
    )
)

get_snowflake_and_lwdb_counts_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id='get_snowflake_lwdb_counts',
        python_callable=get_snowflake_lwdb_counts,
        op_kwargs={
            'prefix': '{{ ts_nodash }}',
            'dry_run': False
        }
    )
)

dag >> generate_snowflake_queries_task >> run_snowflake_queries_task >> get_snowflake_and_lwdb_counts_task
