"""Monthly Identity DAG

This DAG is scheduled on the first day of each month. Spin up a cluster here for more jobs.
"""

from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType

dag = DagHelpers.identity_dag(dag_id="identity-monthly", schedule_interval="13 1 1 * *", start_date=datetime(2024, 9, 1), doc_md=__doc__)

cluster = IdentityClusters.get_cluster("identity-monthly", dag, 200, ComputeType.STORAGE)

cluster.add_sequential_body_task(IdentityClusters.task("com.thetradedesk.idnt.identity.pipelines.Uid2WithKnownCountry", timeout_hours=1))

dag >> cluster

final_dag: DAG = dag.airflow_dag
