from airflow import DAG
from datetime import datetime
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType

dag = DagHelpers.identity_dag(
    dag_id="openpass-etl-agg", schedule_interval="0 6 * * *", start_date=datetime(2024, 7, 19), depends_on_past=True
)

cluster = IdentityClusters.get_cluster("OpenPassETLCluster", dag, 64, ComputeType.STORAGE)

cluster.add_parallel_body_task(IdentityClusters.task("jobs.openpass.OpenPassSignInEventsETL"))
cluster.add_sequential_body_task(IdentityClusters.task("jobs.openpass.OpenPassUserActivitySnapshot"))

check_yesterday = DagHelpers.check_datasets([IdentityDatasets.openpass_signin_events, IdentityDatasets.openpass_user_activity_events])

dag >> check_yesterday >> cluster

final_dag: DAG = dag.airflow_dag
