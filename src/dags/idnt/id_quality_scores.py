"""
Runs the Uid2QualityScorePipeline weekly to populate the Uid2 quality scores.

Eventually this can contain more ID Quality related jobs as well.
"""
from airflow import DAG
from datetime import datetime

from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Executables
from datasources.sources.common_datasources import CommonDatasources

dag = DagHelpers.identity_dag(
    dag_id="id-quality-scores", schedule_interval="5 8 * * MON", start_date=datetime(2025, 6, 23), run_only_latest=True
)

cluster = IdentityClusters.get_cluster("uid2-quality-scores-cluster", dag, 8200, ComputeType.NEW_STORAGE)

cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.Uid2QualityScorePipeline"), timeout_hours=10))

check_input_data = DagHelpers.check_datasets([CommonDatasources.unsampled_identity_avails_daily_agg])

dag >> check_input_data >> cluster

final_dag: DAG = dag.airflow_dag
