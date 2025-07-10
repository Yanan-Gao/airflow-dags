"""
### Graph Bidrelevance Analysis DAG

> DEPRECATED!! PLEASE USE ttdgraph-weekly

This DAG controls the weekly job that calculates avails coverage, both overall and unique, broken out by graph vendor. This data is visualized in the bottom two sections of the "Historical" section of the [Universal Graph Metrics dashboard](https://dash.adsrvr.org/d/mnwC3PgMz/universal-graph-metrics?orgId=1)
"""
from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.statics import Executables, RunTimes, Tags

ttd_env = Tags.environment()
push_to_prometheus = 'true' if str(ttd_env) == 'prod' else 'false'
etl_adbrain_name = "adbrain_legacy"

# Runs every Sunday
dag = DagHelpers.identity_dag(
    dag_id="graph-bidrelevance-analysis",
    schedule_interval="0 22 * * SUN",
    start_date=datetime(2024, 10, 13),
    run_only_latest=False,
    doc_md=__doc__
)

cluster = IdentityClusters.get_cluster("bidrelevance_cluster", dag, 1000, ComputeType.STORAGE)

wait_for_adbrain_graph = DagHelpers.check_datasets(datasets=[IdentityDatasets.get_post_etl_graph(etl_adbrain_name)], timeout_hours=12)

cluster.add_parallel_body_task(
    IdentityClusters.task(
        f"{Executables.identity_repo_class}.graphs.metrics.BidRelevance",
        eldorado_configs=[("graphs.metrics.BidRelevance.graphLookbackWindow", 9),
                          ("graphs.metrics.BidRelevance.uploadToPrometheus", push_to_prometheus)],
        timeout_hours=8,
        runDate=RunTimes.current_full_day
    )
)

dag >> wait_for_adbrain_graph >> cluster

final_dag: DAG = dag.airflow_dag
