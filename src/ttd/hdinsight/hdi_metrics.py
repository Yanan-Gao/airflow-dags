from datetime import datetime
from typing import Optional, Dict, Any, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook

from ttd.hdinsight.hdi_cluster_sensor import HDIClusterSensor
from ttd.hdinsight.hdi_create_cluster_operator import HDICreateClusterOperator
from ttd.hdinsight.hdi_delete_cluster_operator import HDIDeleteClusterOperator
from ttd.hdinsight.livy_sensor import LivySensor
from ttd.hdinsight.livy_spark_submit_operator import LivySparkSubmitOperator
from ttd.ttdenv import TtdEnvFactory

METRICS_DB_CONN_ID = "airflow_metrics_pusher"
INSERT_METRIC_QUERY = """
        INSERT INTO airflow_metrics.hdi_api_stats (dag_id, task_id, cluster_id, step_name, timestamp, error_code, error_message, attempt, execution_date)
        VALUES (%(dag_id)s, %(task_id)s, %(cluster_id)s, %(step_name)s, %(timestamp)s, %(error_code)s, %(error_message)s, %(attempt)s, %(execution_date)s);
    """


def send_hdi_metrics(
    operator: Union[HDICreateClusterOperator, HDIClusterSensor, LivySparkSubmitOperator, LivySensor, HDIDeleteClusterOperator],
    step_name: str,
    context: Dict[str, Any],
    cluster_id: Optional[str] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:

    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
        return

    pg_hook = PostgresHook(
        postgres_conn_id=METRICS_DB_CONN_ID,
        log_sql=False,
    )

    metric_values = {
        "dag_id": operator.dag_id,
        "task_id": operator.task_id,
        "cluster_id": cluster_id if cluster_id is not None else operator.cluster_name,
        "step_name": step_name,
        "timestamp": datetime.now(),
        "error_code": error_code,
        "error_message": error_message,
        "attempt": context["ti"].try_number,
        "execution_date": context["execution_date"],
    }
    pg_hook.run(INSERT_METRIC_QUERY, parameters=metric_values, autocommit=True)
