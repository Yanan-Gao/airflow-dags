import copy
from datetime import timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2

master_fleet_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_12xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

spark_options = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("driver-cores", "4"),
                 ("conf", "spark.driver.memory=10G"), ("conf", "spark.driver.memoryOverhead=3686m"),
                 ("conf", "spark.driver.maxResultSize=100G"), ("conf", "spark.sql.shuffle.partitions=12000"),
                 ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

core_fleet_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_12xlarge().with_ebs_size_gb(750).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5.r5_12xlarge().with_ebs_size_gb(750).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(1500).with_max_ondemand_price().with_fleet_weighted_capacity(2),
        R5a.r5a_24xlarge().with_ebs_size_gb(1500).with_max_ondemand_price().with_fleet_weighted_capacity(2),
        R5d.r5d_24xlarge().with_ebs_size_gb(1500).with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=60
)


def get_max_reach_cluster(cluster_name: str, ) -> EmrClusterTask:
    log_uri = "s3://ttd-ctv/upstream-forecast/validation/logs"
    return EmrClusterTask(
        name=cluster_name,
        log_uri=log_uri,
        core_fleet_instance_type_configs=core_fleet_configs,
        cluster_tags={
            "Team": slack_groups.PFX.team.jira_team,
        },
        master_fleet_instance_type_configs=master_fleet_configs,
        emr_release_label=emr_release_label,
        enable_prometheus_monitoring=True,
    )


def get_maxreach_task(
    cluster: EmrClusterTask, demo_date: str, reach_curve_date: str, timeout: int, env: str, jar_path: str = None
) -> EmrJobTask:
    maxreach_java_options_list = [('ttd.env', env), ('demoDate', demo_date), ('targetDate', reach_curve_date)]

    return EmrJobTask(
        cluster_specs=cluster.cluster_specs,
        name="GenerateMaxReachCountsJobv2",
        class_name="com.thetradedesk.etlforecastjobs.upstreamforecasting.validation.maxreach.GenerateMaxReachCountsJob",
        eldorado_config_option_pairs_list=maxreach_java_options_list,
        additional_args_option_pairs_list=copy.deepcopy(spark_options),
        timeout_timedelta=timedelta(hours=timeout),
        executable_path=jar_path,
    )
