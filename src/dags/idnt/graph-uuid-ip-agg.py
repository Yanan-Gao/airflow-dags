from airflow import DAG
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_datasets import IdentityDatasets
from datetime import datetime
from ttd.ttdenv import TtdEnvFactory

uuid_ip_agg_dag = DagHelpers.identity_dag(dag_id="graph-uuid-ip-agg", start_date=datetime(2024, 9, 3), schedule_interval="0 17 * * SUN")

# TODO get rid of these hacks by reusing pipeline API or ParquetData
write_subdir = 'prod' if TtdEnvFactory.get_from_system().execution_env == 'prod' else 'test'
uiid_ip_meta_agg_checkpoint_path = f's3://ttd-identity/data/{write_subdir}/tmp/uiidIpMetaAgg/graph-checkpoint'

# check last 7 days of data with a lookback - these are daily datasets
check_daily_agg = DagHelpers.check_datasets(
    datasets=[IdentityDatasets.uuid_daily_agg, IdentityDatasets.ip_daily_agg, IdentityDatasets.uuid_ip_daily_agg], lookback=6
)

# daily cluster
cluster = IdentityClusters.get_cluster("uuidIpAggCluster", uuid_ip_agg_dag, 5000, ComputeType.STORAGE)

cluster.add_parallel_body_task(
    IdentityClusters.task(
        "jobs.identity.TTDGraphUiidIpMetaAgg",
        runDate_arg="date",
        eldorado_configs=[('UiidIpMetaAggCheckpointPath', uiid_ip_meta_agg_checkpoint_path), ('GraphLookbackWindowDays', "99")],
        timeout_hours=3
    )
)

uuid_ip_agg_dag >> check_daily_agg >> cluster

final_dag: DAG = uuid_ip_agg_dag.airflow_dag
