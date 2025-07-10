"""
Aggregate identity avails v2 into daily datasets

This is using the etl repo.
"""
from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_datasets import IdentityDatasets

dag = DagHelpers.identity_dag(dag_id="avails-v2-daily-agg", schedule_interval="32 7 * * *", start_date=datetime(2024, 10, 8))

check_identity_avails_v2 = DagHelpers.check_datasets(
    timeout_hours=5,
    datasets=[IdentityDatasets.identity_avails_v2, IdentityDatasets.identity_avails_v2_idless],
)

cluster = IdentityClusters.get_cluster("avails-v2-daily-agg", dag, 3500, ComputeType.STORAGE)

cluster.add_sequential_body_task(IdentityClusters.task("jobs.ingestion.availsdailyagg.AvailsDailyAgg", runDate_arg="Date", timeout_hours=6))

cluster.add_sequential_body_task(IdentityClusters.task("jobs.ingestion.availsdailyagg.AggToIdMapping", runDate_arg="Date"))

dag >> check_identity_avails_v2 >> cluster

final_dag: DAG = dag.airflow_dag
