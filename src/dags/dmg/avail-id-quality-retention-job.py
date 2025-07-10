from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.el_dorado.v2.base import TtdDag
from datetime import datetime

from ttd.tasks.op import OpTask

dag_name = 'AvailIdQuality-RetentionJobs'

sql_connection = 'metadatadb'
schedule_interval = "0 0 * * *"


def AvailIdQuality_MetadataDb_Retention(**kwargs):
    sql_hook = MsSqlHook(mssql_conn_id=sql_connection)
    connection = sql_hook.get_conn()
    connection.autocommit(True)
    cursor = connection.cursor()
    sql_use_db = "use Metadata"
    sql_exec_sproc = "exec dbo.prc_AvailIdQualityRetention"
    cursor.execute(sql_use_db)
    cursor.execute(sql_exec_sproc)


dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 6, 8),
    schedule_interval=schedule_interval,
    slack_channel=DEAL_MANAGEMENT.team.alarm_channel,
    tags=[DEAL_MANAGEMENT.team.jira_team],
    run_only_latest=True
)
adag = dag.airflow_dag

task_AvailIdQuality_MetadataDb_Retention = OpTask(
    op=PythonOperator(task_id="AvailIdQuality-MetadataDb_Retention", python_callable=AvailIdQuality_MetadataDb_Retention, dag=adag)
)

dag >> task_AvailIdQuality_MetadataDb_Retention
