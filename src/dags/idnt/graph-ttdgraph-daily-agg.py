from airflow import DAG
from datetime import datetime
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers, RunTimes
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from datasources.sources.common_datasources import CommonDatasources

dag = DagHelpers.identity_dag(dag_id="graph-ttdgraph-daily-agg", schedule_interval="0 16 * * *", start_date=datetime(2024, 7, 8), retries=1)

cluster = IdentityClusters.get_cluster("TTDGraphDailyAggCluster", dag, 3200, ComputeType.STORAGE)

cluster.add_parallel_body_task(
    IdentityClusters
    .task("jobs.identity.TTDGraphDailyAgg", extra_args=[("conf", "spark.sql.files.ignoreCorruptFiles=true")], runDate_arg="date")
)

check_yesterday = DagHelpers.check_datasets([
    IdentityDatasets.avails_daily, CommonDatasources.rtb_bidfeedback_v5, IdentityDatasets.bidfeedback_eu
])

dag >> check_yesterday >> cluster
dag >> DagHelpers.check_datasets([IdentityDatasets.mobile_walla], RunTimes.days_02_ago_last_hour) >> cluster

final_dag: DAG = dag.airflow_dag
