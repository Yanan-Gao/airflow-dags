# Automated process to enable new Walmart segments for Walmart partners

import logging
from datetime import datetime, timedelta

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from ttd.slack import slack_groups
from ttd.ttdslack import dag_post_to_slack_callback

team_ownership = slack_groups.hpc

dag_id = 'DMP-Provisioning-AddNewWalmartSegmentsToThirdPartyDataTenantAllowList'
task_id = 'DMP_Provisioning_AddNewWalmartSegmentsToThirdPartyDataTenantAllowList'

# see https://airflow.adsrvr.org/connection/list/ access restricted to admin
sql_connection = 'ttd_hpc_airflow'  # provdb-int.adsrvr.org
# sql_connection = 'sandbox_ttd_hpc_airflow'  # intsb-prov-lst.adsrvr.org

db_name = 'Provisioning'

job_schedule_interval = timedelta(days=1)
job_start_date = datetime(2024, 10, 24)

dag = TtdDag(
    dag_id="DMP-Provisioning-AddNewWalmartSegmentsToThirdPartyDataTenantAllowList",
    start_date=job_start_date,
    run_only_latest=True,
    depends_on_past=False,
    schedule_interval=job_schedule_interval,
    tags=[team_ownership.jira_team, 'MSSQL', 'Walmart'],
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_id,
        step_name='DMP_Provisioning_AddNewWalmartSegmentsToThirdPartyDataTenantAllowList',
        slack_channel=team_ownership.alarm_channel,
        message='DAG has failed',
        tsg='https://atlassian.thetradedesk.com/confluence/display/EN/DBJobs'
    )
)

adag = dag.airflow_dag

sql = """
        exec dmp.prc_AddNewProviderSegmentsToThirdPartyDataTenantAllowList
            @providerId = N'walmart2',
            @tenantId = 2,
            @changeSourceId = 3

        declare @brandIds tt_StringIdUnique;
        insert into @brandIds values ('AudienceAccelerator_X'), ('AudienceAccelerator_4'), ('AudienceAccelerator_5'), ('AudienceAccelerator_5_1'), ('AudienceAccelerator_5_Political')

        exec dmp.prc_AddNewProviderSegmentsToThirdPartyDataTenantAllowList
            @providerId = N'thetradedesk',
            @tenantId = 2,
            @changeSourceId = 3,
            @brandIds = @brandIds
    """


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
