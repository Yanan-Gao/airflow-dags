from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import PFX

dag_id = 'ttd-upstream-reach-curve-v2'
job_schedule_interval = None
job_start_date = datetime(2024, 7, 15)
default_static_data_date = "2023-02-13"
default_fmap_version = "v2"

el_dorado_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "fplan", "reach curve"],
    max_active_runs=1
)

dag = el_dorado_dag.airflow_dag

trigger_reach_curve_validation_task = TriggerDagRunOperator(
    dag=el_dorado_dag.airflow_dag,
    task_id="trigger_reach_curve_v2_gen",
    trigger_dag_id="ttd-upstream-reach-curve",
    conf={
        "static_data_date": default_static_data_date,
        "fmap_version": default_fmap_version,
    },
    wait_for_completion=True,
    poke_interval=300,  # 5 min
)

start_task = EmptyOperator(task_id="start", dag=el_dorado_dag.airflow_dag)
end_task = EmptyOperator(task_id="end", dag=el_dorado_dag.airflow_dag)
start_task >> trigger_reach_curve_validation_task >> end_task
