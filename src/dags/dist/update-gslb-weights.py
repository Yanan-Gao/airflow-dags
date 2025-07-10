from datetime import datetime

from airflow.operators.python import PythonOperator
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import dist
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.dist.gslb.update import update_weights

env = TtdEnvFactory.get_from_system()
env_str = str(env).lower()
pipeline_name = "update-gslb-weights"
prometheus_job_name = f'{pipeline_name}-{env_str}'

if env_str == 'prod':
    global_adsrvr_org = 'global.adsrvr.org'
else:
    global_adsrvr_org = f'{env_str}.global.adsrvr.org'

dag = TtdDag(
    dag_id="update-gslb-weights-dag",
    start_date=datetime(2025, 3, 13),
    schedule_interval="*/5 * * * *",
    slack_channel="#scrum-dist-alerts",
    slack_tags=dist.sub_team,
    tags=["DIST"],
    run_only_latest=True,
)

update_weights_task = OpTask(
    op=PythonOperator(
        task_id="update_weights",
        python_callable=update_weights,
        op_kwargs=dict(domain_name=global_adsrvr_org, prometheus_job_name=prometheus_job_name, env=env_str)
    )
)

adag = dag.airflow_dag

dag >> update_weights_task
