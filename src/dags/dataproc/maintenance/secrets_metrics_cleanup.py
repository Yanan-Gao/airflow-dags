from datetime import datetime, timedelta

from airflow import settings
from airflow.operators.python import PythonOperator

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask

dag_name = "airflow-secrets-metrics-cleanup"
interval_to_keep = "30 minutes"

dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 4, 1),
    schedule_interval="21 12 * * *",
    retries=1,
    retry_delay=timedelta(minutes=5),
    run_only_latest=True,
    tags=["DATAPROC", "Maintenance"],
    enable_slack_alert=False,
)
adag = dag.airflow_dag


def delete_old_metrics_data() -> None:
    delete_query = """
    DELETE FROM airflow_metrics.secrets_access WHERE timestamp < now() - CAST(:age AS INTERVAL);
    """
    with settings.Session() as session:
        session.execute(delete_query, {"age": interval_to_keep})
        session.commit()


clean_old_metrics = OpTask(op=PythonOperator(
    task_id="cleanup-old-metrics",
    python_callable=delete_old_metrics_data,
    dag=adag,
))
dag >> clean_old_metrics
