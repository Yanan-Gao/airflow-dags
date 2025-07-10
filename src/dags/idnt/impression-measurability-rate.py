from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType

dag = DagHelpers.identity_dag(
    dag_id="impression-measurability-rate", schedule_interval="0 8 * * *", start_date=datetime(2024, 9, 19), run_only_latest=True
)

cluster = IdentityClusters.get_cluster("impression-measurability-rate-cluster", dag, 650, ComputeType.STORAGE)

cluster.add_parallel_body_task(
    IdentityClusters.task("com.thetradedesk.idnt.identity.metrics.executivedashboard.ImpressionMeasurabilityRate")
)

dag >> cluster

final_dag: DAG = dag.airflow_dag
