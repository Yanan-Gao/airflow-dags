"""
Hourly DAG to execute iSpot MyReports. The iSpot batch is kicked off.
The Kubernetes job polls for new schedule executions, calls iSpot for them, and writes the results to S3, along with successful and failed executions.
All successful executions are written to LogWorkflow DB after the files have been ingested into vertica, which then allows MyReports to execute the vertica queries.
All failed executions are disabled by calling a support sproc on Provisioning DB.
"""

import csv
import logging
from datetime import datetime, timedelta

import jinja2
from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from prometheus_client.metrics import Gauge

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdprometheus import get_or_register_gauge, push_all

job_start_date = datetime(2024, 8, 6, 0, 0)
job_schedule_interval = timedelta(hours=1)

env = TtdEnvFactory.get_from_system()
net_env = 'Production' if env == TtdEnvFactory.prod else 'Development'

pipeline_name = "ctv-ispot-hourly-report-processor"
prometheus_job_name = f'{pipeline_name}-{env}'

logworkflow_connection_mark_success = 'ttd_task_lwdb' if env == TtdEnvFactory.prod else 'sandbox-lwdb'
logworkflow_connection_open_gate = 'lwdb' if env == TtdEnvFactory.prod else 'sandbox-lwdb'
provisioning_connection = 'provisioning-reach-reporter' if env == TtdEnvFactory.prod else 'sandbox-provisioning-reach-reporter'
s3_env = 'prod' if env == TtdEnvFactory.prod else 'test'

docker_image = 'production.docker.adsrvr.org/ttd/ctv/ispot-batch-processor:3.0.0'

# DAG
ttd_dag: TtdDag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["irr"],
    retries=1,
    retry_delay=timedelta(hours=1),
    depends_on_past=False,
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/QMW_Dw"
)
dag: DAG = ttd_dag.airflow_dag

####################################################################################################################
# Steps
####################################################################################################################

# Return true when both vertica east OR vertica west are finished
gating_query_ttd = """
            declare @HourlyTaskId int = 1000071;

            select IIF(COUNT(*) > 0, 1, 0)
            from LogFileTask lft
            where lft.TaskId = @HourlyTaskId
                and LogStartTime = '{{ logical_date.strftime(\"%Y-%m-%d %H:00\") }}'
                and lft.LogFileTaskStatusId = 5 -- dbo.fn_Enum_LogFileTaskStatus_Completed()
                and lft.TaskVariantId IN ( 5, 9 ); -- (5 = dbo.fn_enum_TaskVariant_VerticaUSEast01(), 9 = dbo.fn_enum_TaskVariant_VerticaUSWest01())
            """

gating_query_walmart = """
            declare @HourlyTaskId int = 1000453;

            select IIF(COUNT(*) > 0, 1, 0)
            from LogFileTask lft
            where lft.TaskId = @HourlyTaskId
                and LogStartTime = '{{ logical_date.strftime(\"%Y-%m-%d %H:00\") }}'
                and lft.LogFileTaskStatusId = 5 -- dbo.fn_Enum_LogFileTaskStatus_Completed()
                and lft.TaskVariantId IN ( 24, 25 ); -- (24 = dbo.fn_enum_TaskVariant_VerticaUSEast03(), 25 = dbo.fn_enum_TaskVariant_VerticaUSWest03())
            """

disable_report_template = jinja2.Template(
    """
    declare @machine nvarchar(128);
    declare @scheduleId bigint;
    declare @scheduleExecutionId bigint;

    set @scheduleExecutionId = {{ ScheduleExecutionId }};

    update rptsched.ScheduleExecution
    set ScheduleExecutionStateId = 1, ReportProviderSourceId = {{ ReportProviderSourceId }}
    where ScheduleExecutionId = @scheduleExecutionId;

    set @machine = (select top 1 MachineId from rptsched.ScheduleExecution where ScheduleExecutionId = @scheduleExecutionId);

    EXEC rptsched.prc_FailScheduleExecutionAndSetUserDisabledReason @scheduleExecutionId = @scheduleExecutionId,
                                                                    @machineId = @machine,
                                                                    @statusMessage = null,
                                                                    @userDisabledReason = '{{ DisableReason }}';
    """
)

insert_successful_executions = jinja2.Template(
    """
    declare @HourlyTaskId int = {{ hourly_task_id }};

    {%- for row in object_list %}
    insert into {{ table_name }}(ReportSchedulingEventTypeId, EventData, ReportProviderSourceId, CloudServiceId, DataDomainId)
    select distinct
       162 as ReportSchedulingEventTypeId,
       {{ row[\"ScheduleExecutionId\"] }} as EventData,
       case
           when lft.TaskVariantId = 5 then
               7 -- VerticaUSEast01Cluster
           when lft.TaskVariantId = 9 then
               10 -- VerticaUSWest01Cluster
           when lft.TaskVariantId = 24 then
               15 -- VerticaUsEast03Cluster
           when lft.TaskVariantId = 25 then
               16 -- VerticaUsWest03Cluster
       end as ReportProviderSourceId,
       {{ cloud_service_id }} as CloudServiceId,
       {{ data_domain_id }} as DataDomainId
    from LogFileTask lft
    where lft.TaskId = @HourlyTaskId
        and lft.LogStartTime = '{{ log_start_time }}'
        and lft.LogFileTaskStatusId = 5 -- dbo.fn_Enum_LogFileTaskStatus_Completed()
        and (lft.TaskVariantId IN ( 5, 9 ) -- (5 = dbo.fn_enum_TaskVariant_VerticaUSEast01(), 9 = dbo.fn_enum_TaskVariant_VerticaUSWest01())
        or lft.TaskVariantId IN ( 24, 25 )); -- (24 = dbo.fn_enum_TaskVariant_VerticaUSEast03(), 25 = dbo.fn_enum_TaskVariant_VerticaUSWest03()
    {%- endfor %}
    """
)

prov_db = Secret(
    deploy_type='env',
    deploy_target='ConnectionStrings__Provisioning',
    secret='incremental-reach-reporting-secrets',
    key='REACH_REPORTING_DB_CONNECTION'
)
client_id = Secret(deploy_type='env', deploy_target='iSpot__ClientId', secret='incremental-reach-reporting-secrets', key='ISPOT_CLIENT_ID')
client_secret = Secret(
    deploy_type='env', deploy_target='iSpot__ClientSecret', secret='incremental-reach-reporting-secrets', key='ISPOT_CLIENT_SECRET'
)


def disable_failed_executions_ttd(logical_date, **_):
    disable_failed_executions(logical_date, 'TTD', **_)


def disable_failed_executions_walmart(logical_date, **_):
    disable_failed_executions(logical_date, 'Walmart', **_)


def disable_failed_executions(logical_date, tenant, **_):
    date = str(logical_date.strftime("%Y%m%d"))
    hour = str(logical_date.strftime("%H"))

    logging.info(f'Connecting to {provisioning_connection}')
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_connection, schema='Provisioning')
    conn = sql_hook.get_conn()
    cursor = conn.cursor()

    filekey = f'ispot/{s3_env}/ReachReportMetadata/v=2/date={date}/hour={hour}/FailedExecutions.csv'
    logging.info(f'reading {filekey}')

    file = aws_storage.read_key(filekey, 'ttd-ctv')
    data = csv.DictReader(file.splitlines())
    rows = [row for row in data if row['Tenant'] == tenant]
    report_provider_source_id = 7 if tenant == 'TTD' else 15

    if len(rows) > 0:
        for row in rows:
            logging.info(row)
            sql = disable_report_template.render(row, ReportProviderSourceId=report_provider_source_id)
            logging.info(sql)
            cursor.execute(sql)
            conn.commit()
    else:
        logging.info("No rows found for failed")

    cursor.close()

    failed_executions: Gauge = get_or_register_gauge(
        job=prometheus_job_name,
        name="ispot_report_processor_failed_executions",
        description="Number of iSpot schedule executions that failed for a given hour",
        labels=['tenant']
    )
    failed_executions.labels(tenant=tenant).set(len(rows))
    push_all(prometheus_job_name)


def write_successful_executions_ttd(logical_date, **_):
    write_successful_executions(logical_date, 'TTD', **_)


def write_successful_executions_walmart(logical_date, **_):
    write_successful_executions(logical_date, 'Walmart', **_)


def write_successful_executions(logical_date, tenant, **_):
    date = str(logical_date.strftime("%Y%m%d"))
    hour = str(logical_date.strftime("%H"))
    log_start_time = str(logical_date.strftime("%Y-%m-%d %H:00"))

    logging.info(f'Connecting to {logworkflow_connection_mark_success}')
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    sql_hook = MsSqlHook(mssql_conn_id=logworkflow_connection_mark_success, schema='LogWorkflow')
    conn = sql_hook.get_conn()
    cursor = conn.cursor()

    filekey = f'ispot/{s3_env}/ReachReportMetadata/v=2/date={date}/hour={hour}/SuccessfulExecutions.csv'
    logging.info(f'reading {filekey}')

    file = aws_storage.read_key(filekey, 'ttd-ctv')
    data = csv.DictReader(file.splitlines())
    rows = [row for row in data if row['Tenant'] == tenant]

    hourly_task_id = 1000071
    cloud_service_id = 1
    data_domain_id = 1

    if tenant == 'Walmart':
        hourly_task_id = 1000453
        cloud_service_id = 2
        data_domain_id = 2

    if len(rows) > 0:
        sql = insert_successful_executions.render(
            table_name='rptsched.OutgoingReportSchedulingEvent',
            object_list=rows,
            hourly_task_id=hourly_task_id,
            cloud_service_id=cloud_service_id,
            data_domain_id=data_domain_id,
            log_start_time=log_start_time
        )
        logging.info(sql)
        cursor.execute(sql)
        conn.commit()
    else:
        logging.info("No rows found for success")

    cursor.close()

    successful_executions: Gauge = get_or_register_gauge(
        job=prometheus_job_name,
        name="ispot_report_processor_successful_executions",
        description="Number of iSpot schedule executions that succeeded for a given hour",
        labels=['tenant']
    )
    successful_executions.labels(tenant=tenant).set(len(rows))
    push_all(prometheus_job_name)


def check_for_success(logical_date, **_):
    date = str(logical_date.strftime("%Y%m%d"))
    hour = str(logical_date.strftime("%H"))

    aws_storage = AwsCloudStorage(conn_id='aws_default')
    result = aws_storage.check_for_key(f'ispot/{s3_env}/ReachReport/v=2/date={date}/hour={hour}/_SUCCESS', 'ttd-ctv')
    if not result:
        raise Exception(f'Success file for {logical_date} not found')
    return result


check_report_data = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=check_for_success,
        provide_context=True,
        task_id="check_for_success",
    )
)

report_generator = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='incremental-reach-reporting',
        image=docker_image,
        name="report_generator",
        task_id="report_generator",
        dnspolicy='Default',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        env_vars={
            "DOTNET_ENVIRONMENT": net_env,
            "ASPNETCORE_ENVIRONMENT": net_env
        },
        startup_timeout_seconds=500,
        log_events_on_failure=True,
        service_account_name='incremental-reach-reporting',
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': 'ctv-ispot-hourly-report-processor'
        },
        secrets=[prov_db, client_id, client_secret],
        arguments=["ReportGenerator", "--report-date={{logical_date.strftime(\"%Y-%m-%dT%H:00:00\")}}"],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='2G', limit_memory='4G', request_cpu='1')
    )
)

logworkflow_open_ispot_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'gatingType': 30002,  # dbo.fn_Enum_GatingType_HourlyISpotReachReportProcessing()
                'grain': 100001,  # dbo.fn_Enum_TaskBatchGrain_Hourly()
                'dateTimeToOpen': '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'  # open gate for this hour
            }
        },
        task_id="logworkflow_open_ispot_gate",
    )
)

logworkflow_insert_successful_report_executions = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=write_successful_executions_ttd,
        provide_context=True,
        task_id="logworkflow_insert_successful_executions_ttd",
    )
)

logworkflow_insert_successful_report_executions_walmart = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=write_successful_executions_walmart,
        provide_context=True,
        task_id="logworkflow_insert_successful_executions_walmart",
    )
)

provisioning_disable_failed_report_executions_ttd = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=disable_failed_executions_ttd,
        provide_context=True,
        task_id="provisioning_disable_failed_report_executions_ttd",
    )
)

provisioning_disable_failed_report_executions_walmart = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=disable_failed_executions_walmart,
        provide_context=True,
        task_id="provisioning_disable_failed_report_executions_walmart",
    )
)

logs_gate_sensor_ttd = OpTask(
    op=SqlSensor(
        dag=dag,
        task_id="logs_gate_sensor_ttd",
        conn_id=logworkflow_connection_mark_success,
        sql=gating_query_ttd,
        poke_interval=60 * 5,  # 5 minutes
        timeout=60 * 60 * 4,  # 4 hours
        mode="reschedule"
    )
)

logs_gate_sensor_walmart = OpTask(
    op=SqlSensor(
        dag=dag,
        task_id="logs_gate_sensor_walmart",
        conn_id=logworkflow_connection_mark_success,
        sql=gating_query_walmart,
        poke_interval=60 * 5,  # 5 minutes
        timeout=60 * 60 * 4,  # 4 hours
        mode="reschedule"
    )
)

ttd_dag >> report_generator >> check_report_data >> logworkflow_open_ispot_gate >> logs_gate_sensor_ttd
ttd_dag >> report_generator >> check_report_data >> logworkflow_open_ispot_gate >> logs_gate_sensor_walmart
ttd_dag >> logs_gate_sensor_ttd >> provisioning_disable_failed_report_executions_ttd
ttd_dag >> logs_gate_sensor_ttd >> logworkflow_insert_successful_report_executions
ttd_dag >> logs_gate_sensor_walmart >> provisioning_disable_failed_report_executions_walmart
ttd_dag >> logs_gate_sensor_walmart >> logworkflow_insert_successful_report_executions_walmart
