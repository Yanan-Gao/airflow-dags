import copy
import logging
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
spark_options = [("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                 ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("driver-cores", "4"),
                 ("conf", "spark.driver.memory=11000M"), ("conf", "spark.driver.maxResultSize=3G"),
                 ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

master_fleet_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_12xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5a.r5a_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
        R5a.r5a_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=20
)


def get_validation_pipeline_cluster(cluster_name: str, ) -> EmrClusterTask:
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


def get_validation_pipeline_task(
    cluster: EmrClusterTask,
    date: str,
    env: str,
    job_name: str,
    class_name: str,
    timeout: int,
    jar_path: str = None,
    base_date: str = None,
    need_base_date: bool = False,
    reach_curve_version: str = None
) -> EmrJobTask:
    if reach_curve_version is None:
        java_options_list = [('date', date), ('ttd.env', env)]
    else:
        java_options_list = [('date', date), ('ttd.env', env), ('reachCurveVersion', reach_curve_version)]

    if need_base_date and base_date is not None:
        java_options_list += [('baseDate', base_date)]

    logging.info("java params:" + str(java_options_list))

    return EmrJobTask(
        name=job_name,
        class_name=class_name,
        cluster_specs=cluster.cluster_specs,
        additional_args_option_pairs_list=copy.deepcopy(spark_options),
        eldorado_config_option_pairs_list=java_options_list,
        timeout_timedelta=timedelta(hours=timeout),
        executable_path=jar_path,
    )
