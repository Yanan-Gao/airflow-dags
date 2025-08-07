from datetime import datetime

from airflow.operators.python import PythonOperator

from ttd.eldorado.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id='hotcache-counts-vertica-import',
    start_date=datetime(2024, 7, 5),
    schedule_interval='5 0 * * *',
    slack_channel=hpc.alarm_channel,
    tags=[hpc.jira_team],
    max_active_runs=1
)
adag = dag.airflow_dag

###########################################
# Vertica import HotCache Counts
###########################################
job_environment = TtdEnvFactory.get_from_system()

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'

vertica_import_hotcache_counts = OpTask(
    op=PythonOperator(
        dag=adag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': 10077,  # dbo.fn_Enum_GatingType_ImportHotCacheCounts()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': '{{ logical_date.strftime("%Y%m%d") }}'
            }
        },
        task_id="vertica_import_hotcache_counts",
    )
)

dag >> vertica_import_hotcache_counts >> OpTask(op=FinalDagStatusCheckOperator(dag=adag))
