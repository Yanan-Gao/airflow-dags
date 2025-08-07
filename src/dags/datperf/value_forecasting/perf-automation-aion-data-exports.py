from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.tasks.op import OpTask

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
dag_name = "perf-automation-aion-data-export"

ttd_dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 1, 23),
    schedule_interval='0 23 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/CTayDg',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF'],
    slack_channel="#pod-aion-alerts",
    slack_alert_only_for_prod=True,
)
dag: DAG = ttd_dag.airflow_dag

logworkflow_open_inflight_ag_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection,
            'sproc_arguments': {
                'gatingType':
                2000538,  # ExportAionInFlightAdGroups gate
                'grain':
                100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen':
                '{{ data_interval_start.strftime("%Y-%m-%d") }}',
                'runData':
                '{"DATEOFINTEREST":"{{ (data_interval_end + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}","date":"{{ (data_interval_end + macros.timedelta(days=1)).strftime("%Y%m%d") }}"}'
            }
        },
        task_id="logworkflow_open_inflight_ag_gate",
    )
)

logworkflow_open_hour_of_week_geo_adjustments_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection,
            'sproc_arguments': {
                'gatingType':
                2000545,  # ExportAionHourOfWeekAndGeoAdjustments gate
                'grain':
                100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen':
                '{{ data_interval_start.strftime("%Y-%m-%d") }}',
                'runData':
                '{"DATEOFINTEREST":"{{ (data_interval_end + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}","date":"{{ (data_interval_end + macros.timedelta(days=1)).strftime("%Y%m%d") }}"}'
            }
        },
        task_id="logworkflow_open_hour_of_week_geo_adjustments_gate",
    )
)
