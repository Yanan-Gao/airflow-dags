import pymssql
import time
import sys
import base64
from hashlib import sha256
from datetime import timedelta, datetime
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

dummy_branch = 'xxx-YYYYY-007-branch-to-be-tested'
# -----------------------------------------------------------------------------
#
# Please replace the branch name with your branch that is
# supposed to be tested!!!
#
# In addition, the branch name is used as TestId.
#
# -----------------------------------------------------------------------------
branch_name = dummy_branch

get_schedule_id = """
    select top 1 *
    from ( select top 1000 se.ScheduleId, se.ScheduleExecutionId
           from rptsched.ScheduleExecution se
           inner join rptsched.Schedule s on se.ScheduleId = s.ScheduleId
           inner join rptsched.ReportDefinition rd on s.ReportDefinitionId = rd.ReportDefinitionId
           inner join rptsched.ScheduleDelivery sd on sd.ScheduleId = s.ScheduleId
           left join rptsched.ScheduleFrequencyInMinutes sefmin on s.ScheduleFrequencyInMinutesId = sefmin.ScheduleFrequencyInMinutesId
           left join rptsched.ScheduleFrequencyInMonths sefmon on s.ScheduleFrequencyInMonthsId = sefmon.ScheduleFrequencyInMonthsId
           where se.LastStatusChangeDate > DATEADD(d,0,DATEDIFF(d,0,GETDATE()))
             and se.LastExecutionDurationInSeconds < 300
             and se.ScheduleExecutionStateId = 2
             and rd.MyReportId is not null
             and ( sefmon.SingleExecution is null or sefmon.SingleExecution = 1 )
             and ( sefmin.SingleExecution is null or sefmin.SingleExecution = 1 )
             and s.TestId is null ) as tbl
    order by newid()
"""

is_execution_ready = """
    select se.ScheduleExecutionId
    from rptsched.ScheduleExecution se
    inner join rptsched.ScheduleExecutionQueue seq on se.ScheduleExecutionId = seq.ScheduleExecutionId
    where se.ScheduleId = %d
      and se.ScheduleExecutionStateId = 7
"""


def clone_schedule(ti):

    if branch_name == dummy_branch:
        raise Exception('Please specify a valid branch for testing')

    conninfo = BaseHook.get_connection('provdb_tester')
    print("Got conninfo")
    with pymssql.connect(server=conninfo.host, port=conninfo.port, user=conninfo.login, password=conninfo.password,
                         database=conninfo.schema) as conn:
        with conn.cursor() as cursor:
            # select random ScheduleId / ScheduleExecutionId
            print("get_schedule_id:")
            cursor.execute(get_schedule_id)
            print("Executed get_schedule_ids")
            # clone the schedule
            tpl = cursor.fetchone()
            if tpl is None:
                raise Exception('Could not select schedule')
            scheduleId, scheduleExecutionId = tpl
            print(f"scheduleId={scheduleId}, scheduleExecutionId={scheduleExecutionId}")
            # -----------------------------------------------------------------
            # args is a tuple of following arguments:
            #        - schedule ID
            #        - user name (can be null)
            #        - S3 bucket
            #        - email address (can be null)
            #        - test ID (we use the branch name)
            # -----------------------------------------------------------------
            args = (scheduleId, None, 'thetradedesk-useast-hdreports', None, branch_name)
            cursor.callproc("rptsched.prc_CloneMyReportScheduleAsSingleRun", args)
            print("Executed rptsched.prc_CloneMyReportScheduleAsSingleRun")
            tpl = cursor.fetchone()
            if tpl is None:
                raise Exception('Could not clone schedule')
            (clonedScheduleId, ) = tpl
            print(f"clonedScheduleId={clonedScheduleId}")
            conn.commit()
            # wait for the execution to be in WaitingForExecution
            while True:
                cursor.execute(is_execution_ready, [clonedScheduleId])
                tpl = cursor.fetchone()
                if tpl is not None:
                    (clonedExecutionId, ) = tpl
                    break
                time.sleep(10)
            print(f"clonedExecutionId={clonedExecutionId}")

    ti.xcom_push(key="scheduleId", value=scheduleId)
    ti.xcom_push(key="scheduleExecutionId", value=scheduleExecutionId)
    ti.xcom_push(key="clonedScheduleId", value=clonedScheduleId)
    ti.xcom_push(key="clonedExecutionId", value=clonedExecutionId)


def get_signature(id, d, t, s):
    bytes = bytearray(id.to_bytes(8, sys.byteorder))
    bytes.extend(bytearray(d.to_bytes(8, sys.byteorder)))
    bytes.extend(bytearray(t.to_bytes(8, sys.byteorder)))
    bytes.extend(bytearray(s))
    h = sha256()
    h.update(bytes)
    sign = base64.b64encode(h.digest()).decode("utf-8")
    sign = sign.replace('+', '-')
    sign = sign.replace('/', '_')
    sign = sign.replace('=', '.')
    return sign


def get_url_with_args(id, d, t, secret):
    s = get_signature(id, d, t, secret)
    url = f'https://desk.thetradedesk.com/reports/view/{id}?d={d}&s={s}&t={t}'
    return url


get_secret_and_delivery_id = """
    select sde.Secret, sd.ScheduleDeliveryId
    from rptsched.ScheduleDeliveryExecution sde, rptsched.ScheduleDelivery sd, rptsched.ScheduleExecution se
    where se.ScheduleExecutionId = %d
      and se.ScheduleExecutionId = sde.ScheduleExecutionId
      and se.ScheduleId = sd.ScheduleId
"""


def get_url(id, conn):
    with conn.cursor() as cursor:
        # get secret and delivery id
        cursor.execute(get_secret_and_delivery_id, [id])
        tpl = cursor.fetchone()
        if tpl is None:
            raise Exception('Could not find schedule delivery')
        (secret, d) = tpl
        print(f"d={d}")
        # generate timestamp
        two_hours_from_now = datetime.now() + timedelta(hours=2)
        t = int(two_hours_from_now.timestamp())
        return get_url_with_args(id, d, t, secret)


def set_precision(df, precision):
    from pandas.api.types import is_float_dtype

    for c in df.columns:
        if is_float_dtype(df[c].dtype):
            df[c] = df[c].round(precision)


def compare_reports_xls(url1, url2):
    import pandas as pd

    excel_Sheet_names1 = (pd.ExcelFile(url1)).sheet_names
    excel_Sheet_names2 = (pd.ExcelFile(url2)).sheet_names
    if excel_Sheet_names1 != excel_Sheet_names2:
        raise Exception("Excel reports don't match: diffrent sheets")
    for sheet in excel_Sheet_names1:
        df1 = None
        try:
            df1 = pd.read_excel(url1, sheet)
            set_precision(df1, 10)
        except pd.errors.EmptyDataError:
            pass
        df2 = None
        try:
            df2 = pd.read_excel(url2, sheet)
            set_precision(df2, 10)
        except pd.errors.EmptyDataError:
            pass
        if df1 is None and df2 is None:
            continue
        if df1 is None or df2 is None:
            raise Exception(f"Excel reports don't match (sheet={sheet})")
        if not df1.equals(df2):
            raise Exception(f"Excel reports don't match (sheet={sheet})")


def compare_reports_csv(url1, url2, sep):
    import pandas as pd

    df1 = None
    try:
        df1 = pd.read_csv(url1, sep=sep)
        set_precision(df1, 10)
    except pd.errors.EmptyDataError:
        pass
    df2 = None
    try:
        df2 = pd.read_csv(url2, sep=sep)
        set_precision(df2, 10)
    except pd.errors.EmptyDataError:
        pass
    if df1 is None and df2 is None:
        return
    if df1 is None or df2 is None:
        raise Exception("CSV reports don't match")
    if not df1.equals(df2):
        raise Exception("CSV reports don't match")


get_report_format_id = """
    select sd.ReportFormatId
    from rptsched.ScheduleDelivery sd
    where sd.ScheduleId = %d
"""


def compare_reports(ti):
    conninfo = BaseHook.get_connection('provdb_tester')
    print("Got conninfo")
    scheduleId = ti.xcom_pull(key="scheduleId", task_ids="step1clone")
    scheduleExecutionId = ti.xcom_pull(key="scheduleExecutionId", task_ids="step1clone")
    clonedScheduleId = ti.xcom_pull(key="clonedScheduleId", task_ids="step1clone")
    clonedExecutionId = ti.xcom_pull(key="clonedExecutionId", task_ids="step1clone")
    print(f"scheduleId={scheduleId}, scheduleExecutionId={scheduleExecutionId}")
    print(f"clonedScheduleId={clonedScheduleId}, clonedExecutionId={clonedExecutionId}")

    with pymssql.connect(server=conninfo.host, port=conninfo.port, user=conninfo.login, password=conninfo.password,
                         database=conninfo.schema) as conn:
        with conn.cursor() as cursor:
            # get secret and delivery id
            cursor.execute(get_report_format_id, [scheduleId])
            print("Executed get_report_format_id")
            tpl = cursor.fetchone()
            if tpl is None:
                raise Exception('Could not find report format id')
            (format_id, ) = tpl
        print(f"format_id = {format_id}")

        url1 = get_url(scheduleExecutionId, conn)
        print(f"url1 = {url1}")
        url2 = get_url(clonedExecutionId, conn)
        print(f"url2 = {url2}")

        if format_id == 3 or format_id == 6:  # excel file
            compare_reports_xls(url1, url1)
        elif format_id == 1:  # tsv
            compare_reports_csv(url1, url2, '\t')
        elif format_id == 4:  # csv
            compare_reports_csv(url1, url2, ',')


alarm_slack_channel = '#scrum-dp-rpts-alerts'
dpsr_rpt_linux_test = 'dpsr-rpt-linux-test'

default_args = {
    'owner': 'DPRPTS',
}

dag = TtdDag(
    dag_id=dpsr_rpt_linux_test,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=20),
    slack_channel=alarm_slack_channel,
    schedule_interval=None,  # if you want to run the test periodically provide a schedule
    max_active_runs=1,
    run_only_latest=False,
    depends_on_past=True,
    start_date=datetime(2024, 6, 4, 5, 0, 0),
)

adag = dag.airflow_dag

# -----------------------------------------------------------------------------
#
# Step 1: Select and clone schedule for testing.
#
# -----------------------------------------------------------------------------
step1clone = OpTask(op=PythonOperator(task_id="step1clone", python_callable=clone_schedule))

# -----------------------------------------------------------------------------
#
# Step 2: Generate the report using respective branch
#
# -----------------------------------------------------------------------------
step2execute = OpTask(
    op=TaskServiceOperator(
        task_name="ScheduledReportingTask",
        task_config_name="ScheduledReportingTaskConfig",
        scrum_team=dprpts,
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "TestId": branch_name,
            "ScheduledReporting.Enabled": "True",
            "ScheduledReporting.AllowedReportProviderSourceIds": "3,4,8,9,13,101,103",
            "ScheduledReporting.RunOnce": "true"
        },
        branch_name=branch_name,
    )
)

# -----------------------------------------------------------------------------
#
# Step 3: Compare original and newly generated reports
#
# -----------------------------------------------------------------------------

step3compare = OpTask(op=PythonOperator(task_id="step3compare", python_callable=compare_reports))

dag >> step1clone >> step2execute >> step3compare
