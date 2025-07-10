from airflow import DAG
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_datasets import IdentityDatasets
from datetime import datetime

sea_graph_dag = DagHelpers.identity_dag(
    dag_id="graph-ttd-sea", start_date=datetime(2024, 8, 31), schedule_interval="0 14 * * *", retries=1, depends_on_past=True
)

check_identity_avails = DagHelpers.check_datasets([IdentityDatasets.avails_daily])

# daily cluster
daily_cluster = IdentityClusters.get_cluster("TTDGraphSEADailyCluster", sea_graph_dag, 500, ComputeType.STORAGE)

daily_cluster.add_parallel_body_task(IdentityClusters.task("jobs.identity.seagraph.SEAAvailsAgg", runDate_arg="date"))

# on Tuesdays, we create scores from daily aggs
weekly_cluster = IdentityClusters.get_cluster("TTDGraphSEAScorerCluster", sea_graph_dag, 1500, ComputeType.STORAGE)
weekly_cluster.add_parallel_body_task(IdentityClusters.task("jobs.identity.seagraph.SEAEdgeGenerator", runDate_arg="date"))
weekly_cluster.add_parallel_body_task(IdentityClusters.task("jobs.identity.seagraph.SEAEdgeScorer", timeout_hours=12, runDate_arg="date"))

# need a dummy task here to branch to
tuesday_only = DagHelpers.skip_unless_day(["Tuesday"])

sea_graph_dag >> check_identity_avails >> daily_cluster >> tuesday_only >> weekly_cluster

final_dag: DAG = sea_graph_dag.airflow_dag
