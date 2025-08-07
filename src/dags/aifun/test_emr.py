from datetime import datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.openlineage import OpenlineageConfig, OpenlineageAssetsConfig, OpenlineageTransport

# DAG Configuration
job_start_date = datetime(year=2024, month=5, day=24, hour=10, minute=0)

test_openlineage_dag: TtdDag = TtdDag(
    dag_id="test_openlineage_emr",
    start_date=datetime(year=2024, month=4, day=1, hour=10, minute=0),
    schedule_interval=None,
    retries=0,
    slack_tags="AIFUN",
    enable_slack_alert=False,
)

# -- Configuration --
# Openlineage config
# Job config
# -- If building from another repo, you may need to change the built_jar_bucket
built_jar_path = "mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/ds/libs/sparktestjob-sbi-aifun-1415-create-exception-test-assembly.jar"
test_class_name = "com.thetradedesk.spark.Hello"
built_jar_bucket = "thetradedesk-mlplatform-us-east-1"

# -- Below, avoid changing (sets up the dag)` --


def get_test_jar_location(jar_path: str) -> str:
    return f"s3://{built_jar_bucket}/{jar_path}"


master_fleet = EmrFleetInstanceTypes(
    instance_types=[C5.c5_2xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(10)],
    on_demand_weighted_capacity=1,
)

worker_fleet = [C5.c5_2xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(10)]

cluster = EmrClusterTask(
    name="OpenlineageTest",
    master_fleet_instance_type_configs=master_fleet,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=worker_fleet,
        on_demand_weighted_capacity=1,
    ),
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        },
    }],
    cluster_tags={
        "Team": "AIFUN",
        "Process": "OpenlineageTest"
    },
    enable_prometheus_monitoring=True,
    enable_spark_history_server_stats=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    custom_java_version=11,
    region_name="us-east-1",
    retries=0
)

etl_step = EmrJobTask(
    name="OpenlineageTestJobEMR",
    class_name=test_class_name,
    executable_path=get_test_jar_location(built_jar_path),
    additional_args_option_pairs_list=[],
    cluster_specs=cluster.cluster_specs,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[],
    region_name="us-east-1",
    openlineage_config=OpenlineageConfig(
        transport=OpenlineageTransport.ROBUST,
        assets_config=OpenlineageAssetsConfig(branch="mwp-AIFUN-1617-writer-closing-early-object-mapper")
    )
)

cluster.add_parallel_body_task(etl_step)
test_openlineage_dag >> cluster

adag = test_openlineage_dag.airflow_dag
