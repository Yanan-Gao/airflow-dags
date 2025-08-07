import logging
from datetime import datetime, timedelta

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from ttd.slack import slack_groups
from ttd.ttdslack import dag_post_to_slack_callback

team_ownership = slack_groups.hpc

# purge job that cleans up and deletes old ThirdPartyDataSearchRecordsMetadata batches
dag_id = 'DMP-Provisioning-PurgeThirdPartyDataSearchRecordsMetadataDeltaV3'
task_id = 'DMP_Provisioning_PurgeThirdPartyDataSearchRecordsMetadataDeltaV3'  # DAG of one task

# see https://airflow.adsrvr.org/connection/list/ access restricted to admin
sql_connection = 'ttd_hpc_airflow'  # provdb-int.adsrvr.org
# sql_connection = 'sandbox_ttd_hpc_airflow'  # intsb-prov-lst.adsrvr.org

db_name = 'Provisioning'

job_schedule_interval = timedelta(hours=3)  # run the purge job 8 times a day
# schedule_interval = None # for testing

job_start_date = datetime(2024, 10, 31)

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    run_only_latest=True,
    depends_on_past=False,
    schedule_interval=job_schedule_interval,
    tags=[team_ownership.jira_team, 'MSSQL'],
    max_active_runs=1,
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_id,
        step_name=task_id,
        slack_channel=team_ownership.alarm_channel,
        message='DAG has failed',
        tsg='https://atlassian.thetradedesk.com/confluence/display/EN/DBJobs'
    )
)

adag = dag.airflow_dag

sql = "exec dmp.prc_PurgeThirdPartyDataSearchRecordsMetadataDeltaV3 @keepDays = 2"


def dmp_Provisioning_RunSqlStatement():
    connection = BaseHook.get_connection(sql_connection)
    hook = connection.get_hook()
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    logging.info(f"Executing the following statements:\n{sql}")
    cursor.execute(sql)
    cursor.close()
    logging.info(f"DAG, {dag_id}, has completed")


task_1 = OpTask(op=PythonOperator(task_id=task_id, python_callable=dmp_Provisioning_RunSqlStatement))

dag >> task_1
