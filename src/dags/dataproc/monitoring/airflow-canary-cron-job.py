from datetime import datetime, timedelta
from time import time

from airflow.models import Variable
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.tasks.python import PythonOperator
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge
from ttd.workers.worker import Workers

default_args = {
    "owner": "dataproc",
    "start_date": datetime(2023, 5, 11),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

dag_info = [["airflow-canary-cron", Workers.celery]]

for dag_name, worker in dag_info:
    dag = TtdDag(
        dag_id=dag_name,
        default_args=default_args,
        schedule_interval="* * * * *",
        tags=["Canary", "Monitoring", "Operations"],
        run_only_latest=True,
    )

    def push_metrics(**context):
        execution_date = int(context["execution_date"].int_timestamp)
        interval_end_date = int(context["data_interval_end"].int_timestamp)
        current_time = time()

        gauge_labels = {"queue": worker.queue}

        schedule_time: TtdGauge = get_or_register_gauge(
            job=dag_name,
            name="airflow_canary_dag_scheduled_time_seconds",
            description="The epoch time the canary DAG should be running at",
        )
        schedule_time.labels(gauge_labels).set(execution_date)

        run_time: TtdGauge = get_or_register_gauge(
            job=dag_name,
            name="airflow_canary_dag_run_time_seconds",
            description="The epoch time the canary DAG actually ran it's step",
        )
        run_time.labels({"dag_url": f"{Variable.get('BASE_URL')}/dags/{dag_name}/grid", **gauge_labels}).set(current_time)

        latency: TtdGauge = get_or_register_gauge(
            job=dag_name,
            name="airflow_canary_dag_latency_seconds",
            description="Latency of the run time of the canary DAG",
        )
        latency.labels(gauge_labels).set(current_time - interval_end_date)

        push_all(job=dag_name)

    push = OpTask(op=PythonOperator(
        task_id="push_canary_metrics",
        python_callable=push_metrics,
        queue=worker.queue,
        pool=worker.pool,
    ))

    dag >> push

    globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
