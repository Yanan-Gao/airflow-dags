import json
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import MEASUREMENT_UPPER
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.task_service_dag import TaskServiceDagFactory
from ttd.ttdenv import TtdEnvFactory

env = TtdEnvFactory.get_from_system()

log_start_time = "{{ data_interval_start.strftime('%Y-%m-%d %H:00:00') }}"

task_name = "ComscoreCcrReportPullTask"
task_config_name = "ComscoreCcrReportPullTaskConfig"

task_service_dag = TaskServiceDagFactory(
    task_name=task_name,
    task_config_name=task_config_name,
    scrum_team=MEASUREMENT_UPPER.team,
    start_date=datetime(2024, 6, 1),
    job_schedule_interval="30 * * * *",
    resources=TaskServicePodResources.medium(),
    task_execution_timeout=timedelta(hours=2),
    run_only_latest=True,
    task_data=json.dumps({
        "LogStartTime": log_start_time,
    }),
    configuration_overrides={
        "WriteMetricsToDatabase": "False"
    }
).create_dag()

dag = task_service_dag.airflow_dag

# DataMover Config
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"

logworkflow_connection_open_gate = logworkflow_connection if env == TtdEnvFactory.prod \
    else logworkflow_sandbox_connection

# Opens gate for weekly DataMover task to import data into ProvDB from S3
data_mover_task_id = "logworkflow_open_add_comscore_ccr_report_gate"
logworkflow_open_comscore_ccr_report_gate = PythonOperator(
    dag=dag,
    python_callable=ExternalGateOpen,
    provide_context=True,
    op_kwargs={
        'mssql_conn_id': logworkflow_connection_open_gate,
        'sproc_arguments': {
            'gatingType': 2000334,  # dbo.fn_Enum_GatingType_ImportComscoreCcrReport()
            'grain': 100001,  # dbo.fn_Enum_TaskBatchGrain_Hourly()
            'dateTimeToOpen': log_start_time  # open gate for this hour
        }
    },
    task_id=data_mover_task_id,
    retries=3,
    retry_delay=timedelta(minutes=5)
)

dag.set_dependency(TaskServiceOperator.format_task_name(task_name, None), data_mover_task_id)

dag
