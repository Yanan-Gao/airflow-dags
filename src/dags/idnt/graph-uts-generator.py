from airflow import DAG
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from datetime import datetime
from ttd.ttdenv import TtdEnvFactory
from dags.idnt.identity_helpers import PathHelpers

# TODO remove this once on new datapipeline API, clusters append runName already
# to be compatible with identity repo, which uses runName to differentiate test/prod runs
# we need to modify the pre_uid2 path (intermediate path) accordingly
if TtdEnvFactory.get_from_system().execution_env == "prod":
    intermediate_write_subdir = ""
    final_write_subdir = ""
else:
    intermediate_write_subdir = f"/runs/runName={PathHelpers.get_test_airflow_run_name().split('::')[-1]}"
    final_write_subdir = "/test"

uts_pre_uid2_output_path = f"s3://thetradedesk-useast-data-import{intermediate_write_subdir}/sxd-etl/deterministic/uts_graph/pre_uid2"
uts_with_uid2_output_path = f"s3://thetradedesk-useast-data-import/sxd-etl{final_write_subdir}/deterministic/uts_graph/devices/v=1/"

uts_dag = DagHelpers.identity_dag(dag_id="graph-uts-generator", start_date=datetime(2024, 9, 1), schedule_interval="0 3 * * SUN")

# daily cluster
cluster = IdentityClusters.get_cluster("UTSGraphGeneratorCluster", uts_dag, 3000, ComputeType.STORAGE)

cluster.add_sequential_body_task(
    IdentityClusters.task(
        "jobs.identity.UTSGraphGenerator",
        runDate_arg="date",
        eldorado_configs=[("utsOutputPath", uts_pre_uid2_output_path)],
        timeout_hours=6
    )
)

cluster.add_sequential_body_task(
    IdentityClusters.task(
        "com.thetradedesk.idnt.identity.graphs.uts.Uid2Merge",
        eldorado_configs=[("graphs.uts.Uid2Merge.utsFinalPath", uts_with_uid2_output_path)],
        timeout_hours=1
    )
)

uts_dag >> cluster

final_dag: DAG = uts_dag.airflow_dag
