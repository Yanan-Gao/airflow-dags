from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6i import R6i
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DATPERF, AUDAUTO
from dags.datperf.utils.spark_config_utils import get_spark_args
from dags.datperf.datasets import geronimo_dataset
from ttd.tasks.op import OpTask

import copy

# Instance configuration
instance_type = R6i.r6i_8xlarge()
on_demand_weighted_capacity = 100

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)
spark_args = get_spark_args(cluster_params)

# Jar
PLUTUS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar"

# Lookback
LOOKBACK = 7

plutus_etl = TtdDag(
    dag_id="perf-automation-plutus-etl",
    start_date=datetime(2024, 9, 4),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/yrMMCQ',
    retries=0,
    run_only_latest=False,
    retry_delay=timedelta(hours=1),
    default_args={"owner": "DATPERF"},
    enable_slack_alert=False,
    tags=["DATPERF", "Plutus"],
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutus_etl.airflow_dag

geronimo_etl_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='geronimo_etl_available',
        poke_interval=60 * 30,
        timeout=60 * 60 * 12,
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        datasets=[geronimo_dataset]
    )
)

plutus_etl_cluster = EmrClusterTask(
    name="PlutusEtlCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[instance_type.with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6i.r6i_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R6i.r6i_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
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

raw_data_step = EmrJobTask(
    name="RawDataIngestion",
    class_name="job.RawInputDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}"),
                                       ('outputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/')],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

clean_data_step = EmrJobTask(
    name="PlutusCleanData",
    class_name="job.CleanInputDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}"),
                                       ('inputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/'),
                                       ('outputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/')],
    executable_path=PLUTUS_JAR
)

# this requires there to be at least daysOfDat number of clean data in order to succeed. T
model_input_step = EmrJobTask(
    name="PlutusModelInput",
    class_name="job.ModelInputProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ],
    eldorado_config_option_pairs_list=[
        ('date', "{{ds}}"), ('inputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/'),
        ('outputPath', 's3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/'), ('daysOfDat', '7'),
        ('onlyWriteSingleDay', 'true')
    ],
    executable_path=PLUTUS_JAR
)

plutus_etl_cluster.add_sequential_body_task(raw_data_step)
plutus_etl_cluster.add_sequential_body_task(clean_data_step)
plutus_etl_cluster.add_sequential_body_task(model_input_step)

plutus_etl >> geronimo_etl_sensor >> plutus_etl_cluster
