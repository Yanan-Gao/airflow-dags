"""
Runs the Uid2QualityScorePipeline weekly to populate the Uid2 quality scores.

Eventually this can contain more ID Quality related jobs as well.
"""
from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Executables, Tags
from datasources.sources.common_datasources import CommonDatasources
from ttd.interop.logworkflow_callables import ExecuteOnDemandDataMove
from ttd.tasks.op import OpTask

ttd_env = Tags.environment()

# LWDB configs
# NOTE: The sandbox connection currently isn't supported, but I'm leaving it this way so we don't accidentally trigger
# a production DataMover job on a past date or something while testing. This means the logworkflow task will fail when
# testing this dag in the prodTest environment.
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"
logworkflow_connection_open_gate = logworkflow_connection if ttd_env == "prod" \
    else logworkflow_sandbox_connection

dag = DagHelpers.identity_dag(
    dag_id="id-quality-scores", schedule_interval="5 8 * * MON", start_date=datetime(2025, 6, 23), run_only_latest=True
)

cluster = IdentityClusters.get_cluster("uid2-quality-scores-cluster", dag, 8200, ComputeType.STORAGE)

cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.Uid2QualityScorePipeline"), timeout_hours=10))

logworkflow_open_id_quality_sql_import_gate = OpTask(
    op=PythonOperator(
        dag=dag.airflow_dag,
        python_callable=ExecuteOnDemandDataMove,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'taskId': 1000813,  # dbo.fn_enum_Task_OnDemandImportIdQualityScoreFromCloud();
                'prefix': """date={{ (data_interval_end.start_of('day') - macros.timedelta(days=1)).format('YYYYMMDD') }}/"""
            }
        },
        task_id="logworkflow_open_id_quality_sql_import_gate",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
)

check_input_data = DagHelpers.check_datasets([CommonDatasources.unsampled_identity_avails_daily_agg])

dag >> check_input_data >> cluster >> logworkflow_open_id_quality_sql_import_gate

final_dag: DAG = dag.airflow_dag
