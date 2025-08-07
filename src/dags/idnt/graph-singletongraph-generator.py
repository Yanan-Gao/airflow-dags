from airflow import DAG
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_datasets import IdentityDatasets
from datetime import datetime

dag = DagHelpers.identity_dag(dag_id="graph-singletongraph-generator", start_date=datetime(2024, 9, 1), schedule_interval="0 19 * * SUN")

check_datasets = DagHelpers.check_datasets([
    IdentityDatasets.GraphCheckPoints.ipMeta,
    IdentityDatasets.GraphCheckPoints.uuidIpDate,
    IdentityDatasets.GraphCheckPoints.uuidMeta,
])

cluster = IdentityClusters.get_cluster("SingletonGraphGeneratorCluster", dag, 5000, ComputeType.STORAGE)

cluster.add_parallel_body_task(
    IdentityClusters.task(
        "jobs.identity.singletongraph.TTDSingletonGraphGenerator",
        runDate_arg="date",
        timeout_hours=3,
    )
)

dag >> check_datasets >> cluster

final_dag: DAG = dag.airflow_dag
