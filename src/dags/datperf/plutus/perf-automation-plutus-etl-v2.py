from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6i import R6i
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DATPERF, AUDAUTO
from dags.datperf.datasets import plutus_dataset
from dags.datperf.utils.spark_config_utils import get_spark_args
from ttd.tasks.op import OpTask

import copy

from ttd.ttdenv import TtdEnvFactory

# Instance configuration
instance_type = R6i.r6i_8xlarge()
on_demand_weighted_capacity = 120

# Spark configuration
cluster_params = instance_type.calc_cluster_params(
    instances=on_demand_weighted_capacity, min_executor_memory=180, max_cores_executor=32, memory_tolerance=0.95
)
spark_args = get_spark_args(cluster_params) + \
             [("conf", "spark.memory.fraction=0.7"),  # Increases memory available to spark
              ("conf", "spark.memory.storageFraction=0.25")]

# Jar
PLUTUS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar"

# Testing config
TEST_BRANCH = None
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest and TEST_BRANCH is not None:
    PLUTUS_JAR = f"s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/mergerequests/{TEST_BRANCH}/latest/plutus.jar"

# Lookback
LOOKBACK = 7

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
plutus_etl_v2 = TtdDag(
    dag_id="perf-automation-plutus-etl-v2",
    start_date=datetime(2024, 7, 16),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/yrMMCQ',
    retries=0,
    run_only_latest=False,
    tags=['DATPERF', "Plutus"],
    enable_slack_alert=False,
    default_args={"owner": "DATPERF"},
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutus_etl_v2.airflow_dag

plutus_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='plutus_dataset_available',
        poke_interval=60 * 30,
        timeout=60 * 60 * 12,
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        datasets=[plutus_dataset]
    )
)

plutus_etl_v2_cluster = EmrClusterTask(
    name="PlutusEtlV2Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R6i.r6i_8xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6i.r6i_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R6i.r6i_16xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R6i.r6i_32xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=on_demand_weighted_capacity,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }],
    enable_prometheus_monitoring=True
)

im_data_step = EmrJobTask(
    name="ImplicitData",
    class_name="job.PlutusImplicitDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[
        ('date', "{{ds}}"), ('outputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus'),
        ('featuresJson', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/dev/schemas/features.json')
    ],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

ex_data_step = EmrJobTask(
    name="ExplicitData",
    class_name="job.PlutusExplicitDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[
        ('date', "{{ds}}"), ('outputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus'),
        ('featuresJson', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/dev/schemas/features.json')
    ],
    executable_path=PLUTUS_JAR
)

plutus_etl_v2_cluster.add_parallel_body_task(im_data_step)
plutus_etl_v2_cluster.add_parallel_body_task(ex_data_step)

plutus_etl_v2 >> plutus_dataset_sensor >> plutus_etl_v2_cluster
