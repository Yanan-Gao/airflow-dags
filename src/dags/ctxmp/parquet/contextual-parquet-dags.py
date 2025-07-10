from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Tuple

from dags.ctxmp import constants
from dags.ctxmp.parquet.contextual_parquet_utils import LogNames, CloudJobConfig
from ttd.cluster_service import ClusterServices
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes, EmrInstanceType
from ttd.slack import slack_groups

job_schedule_interval = "0 * * * *"

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]


def get_additional_args(job_config: CloudJobConfig):
    return [
        ("conf", f"spark.driver.maxResultSize={job_config.max_result_size_gb}g"),
        ("conf", f"spark.sql.shuffle.partitions={job_config.parallelism}"),
    ]


def create_dag(logname: str, job_config: CloudJobConfig):
    # Calculate the previous hour start date
    previous_hour_start = '{{ (data_interval_start - macros.timedelta(hours=1)).strftime(\"%Y-%m-%dT%H:00\") }}'

    logging.info(f"job_config.provider: {job_config.provider}, clusterService: {job_config.cluster_service}")

    contextual_dag = TtdDag(
        dag_id=f"contextual-{logname.lower()}-{job_config.provider.__str__()}",
        start_date=job_config.start_date,
        schedule_interval=job_schedule_interval,
        tags=[slack_groups.CTXMP.team.name, slack_groups.CTXMP.team.jira_team],
        max_active_runs=8,
        slack_tags=slack_groups.CTXMP.team.name,
        slack_channel=slack_groups.CTXMP.team.alarm_channel,
        enable_slack_alert=True,
    )

    etl_cluster = get_cluster_task(f"contextual-{logname.lower()}", job_config)
    pipeline_step = get_job_task(logname, job_config, previous_hour_start)
    etl_cluster.add_parallel_body_task(pipeline_step)

    # Define the DAG sequence
    contextual_dag >> etl_cluster

    return contextual_dag


def get_cluster_task(name: str, job_config: CloudJobConfig) -> EmrClusterTask:
    etl_cluster = None
    cluster_tags = {
        "Cloud": job_config.provider.__str__(),
        "Team": slack_groups.CTXMP.team.jira_team,
    }

    if job_config.cluster_service == ClusterServices.AwsEmr:
        etl_cluster = EmrClusterTask(
            name=name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=default_master_fleet_instance_types,
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=default_core_fleet_instance_types(job_config),
                on_demand_weighted_capacity=job_config.capacity,
            ),
            emr_release_label=constants.aws_emr_version,
            enable_prometheus_monitoring=True,
            cluster_auto_terminates=True,
            cluster_tags=cluster_tags,
        )

    return etl_cluster


def get_job_task(logname: str, job_config: CloudJobConfig, previous_hour_start: str) -> EmrJobTask:
    pipeline_step = None
    task_name = "EtlPipeline"
    class_name = f"jobs.contextual.parquet.{logname}EtlPipeline"
    common_eldorado_config = java_settings_list + [
        ("runtime", previous_hour_start),
        ("ttd.defaultcloudprovider", job_config.provider.__str__()),
        ("coalesceSize", job_config.output_files),
        ("logLevel", "DEBUG"),
    ]

    if job_config.cluster_service == ClusterServices.AwsEmr:
        pipeline_step = EmrJobTask(
            name=task_name,
            executable_path=constants.eldorado_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config,
            additional_args_option_pairs_list=get_additional_args(job_config),
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )
    return pipeline_step


default_master_fleet_instance_types: List[EmrInstanceType] = [M7g.m7g_2xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)]


def default_core_fleet_instance_types(job_config: CloudJobConfig) -> List[EmrInstanceType]:
    # yapf: disable
    # Add EBS to the instance types that don't have built-in SSD in the image
    _16xlarge = [
        R7gd.r7gd_16xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(64),
        R6gd.r6gd_16xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(64),
    ]
    _12xlarge = [
        R7gd.r7gd_12xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(48),
        R6gd.r6gd_12xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(48),
    ]
    _8xlarge = [
        R7gd.r7gd_8xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(32),
        R6gd.r6gd_8xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(32),
    ]
    _4xlarge = [
        R7gd.r7gd_4xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(16),
        R6gd.r6gd_4xlarge()
            .with_max_ondemand_price()
            .with_fleet_weighted_capacity(16),
    ]
    # yapf: enable

    if job_config.capacity < 16:
        raise Exception(f'Capacity must be 16 or above, but is {job_config.capacity}')
    elif job_config.capacity % 16 != 0:
        raise Exception(f'Capacity must be divisible by 16, but is {job_config.capacity}')
    elif job_config.capacity > 1000:
        return _12xlarge + _16xlarge
    else:
        return _4xlarge + _8xlarge + _12xlarge


# yapf: disable
dag_info: List[Tuple[str, List[CloudJobConfig]]] = [
    (
        LogNames.blockedByPreBidReasons,
        [CloudJobConfig(
            start_date=datetime(2025, 1, 2, 00, 00),
            capacity=640,
            parallelism=300,
            base_disk_space=100,
            output_files=160)]
    ),
    (
        LogNames.referrerCacheAvailableCategories,
        [CloudJobConfig(
            start_date=datetime(2025, 1, 2, 00, 00),
            capacity=3648,
            parallelism=2000,
            base_disk_space=400,
            output_files=1824,
            max_result_size_gb=5)]
    ),
    (
        LogNames.referrerCacheAvailableRequestData,
        [CloudJobConfig(
            start_date=datetime(2025, 1, 2, 00, 00),
            capacity=1920,
            parallelism=1000,
            base_disk_space=200,
            output_files=640)],
    ),
    (
        LogNames.referrerCacheProviderRequestData,
        [CloudJobConfig(
            start_date=datetime(2025, 1, 24, 00, 00),
            capacity=1920,
            parallelism=1000,
            base_disk_space=200,
            output_files=640)],
    ),
    (
        LogNames.adGroupContextualEntityPerformance,
        [CloudJobConfig(
            start_date=datetime(2025, 7, 8, 00, 00),
            capacity=640,
            parallelism=300,
            base_disk_space=100,
            output_files=160)]
    )
]
# yapf: enable

for (logname, cloud_configs) in dag_info:
    config: CloudJobConfig
    for config in cloud_configs:
        dag = create_dag(logname, config)
        globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
