"""
Runs the AlternativeIdMetrics pipeline weekly to compute metrics that will allow us to evaluate the potential of
external user IDs in avails.
"""
from airflow import DAG
from datetime import datetime

from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Executables
from datasources.datasources import Datasources

dag = DagHelpers.identity_dag(
    dag_id="alternative-id-metrics", schedule_interval="5 8 * * MON", start_date=datetime(2025, 7, 7), run_only_latest=True
)

cluster = \
    IdentityClusters.get_cluster("alternative_id_metrics_cluster", dag, 750, ComputeType.STORAGE)

cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.AlternativeIdMetrics"), timeout_hours=2))

check_avails = DagHelpers.check_datasets([Datasources.avails.avails_30_day])

dag >> check_avails >> cluster

final_dag: DAG = dag.airflow_dag
