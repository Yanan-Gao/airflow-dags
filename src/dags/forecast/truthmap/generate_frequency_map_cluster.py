from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from dags.forecast.utils.root_locations_s3 import RootLocationS3

from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack import slack_groups
from ttd.ttdenv import TtdEnvFactory


class MapType(Enum):
    FrequencyMap = "frequency-maps"
    TruthMap = "truth-maps"


@dataclass(frozen=True)
class ClusterSettings:
    partition_count: int
    weighted_capacity: int


DEFAULT_FMAP_CLUSTER_SETTINGS = ClusterSettings(12000, 60 * 96)
LARGE_FMAP_CLUSTER_SETTINGS = ClusterSettings(24000, 2 * 60 * 96)


def get_generate_frequency_map_cluster(
    map_type: MapType,
    avail_stream: str,
    id_type: str,
    mapping_types: str,
    time_ranges: tuple,
    target_date: str,
    jar_path: str,
    emr_release_label: str,
    timeout: int,
    task_id_suffix: str = "",
    additional_config: list = [],
    cluster_settings: ClusterSettings = DEFAULT_FMAP_CLUSTER_SETTINGS,
    graph_name=str
) -> EmrClusterTask:
    partitions = cluster_settings.partition_count

    job_name = '-'.join([f'generate-{map_type.value}{task_id_suffix}', avail_stream, mapping_types, id_type])
    job_environment = TtdEnvFactory.get_from_system()
    log_uri = f"{RootLocationS3.FORECASTING_ROOT}/env={job_environment.dataset_write_env}/{map_type.value}/logs"

    spark_options_list = [("executor-memory", "205G"), ("executor-cores", "30"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=200G"),
                          ("conf", "spark.driver.maxResultSize=6G"), ("conf", f"spark.sql.shuffle.partitions={partitions}"),
                          ("conf", "spark.yarn.maxAppAttempts=1")]

    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
            R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
        ],
        on_demand_weighted_capacity=cluster_settings.weighted_capacity,
    )

    generate_frequency_map_cluster = EmrClusterTask(
        name=job_name,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_release_label,
        cluster_tags={
            "Team": slack_groups.FORECAST.team.jira_team,
        },
        enable_prometheus_monitoring=True,
        log_uri=log_uri,
        use_on_demand_on_timeout=True
    )

    generate_frequency_map_step = EmrJobTask(
        name="generate-frequency-map",
        class_name="com.thetradedesk.etlforecastjobs.universalforecasting.frequencymap.generation.FrequencyMapGeneratingJob",
        executable_path=jar_path,
        additional_args_option_pairs_list=spark_options_list,
        eldorado_config_option_pairs_list=[('targetDate', target_date), ('mappingTypes', mapping_types), ('availStream', avail_stream),
                                           ('idType', id_type), ('timeRanges', ','.join(map(str, time_ranges))),
                                           ("graphName", graph_name)] + additional_config,
        timeout_timedelta=timedelta(hours=timeout)
    )
    generate_frequency_map_cluster.add_parallel_body_task(generate_frequency_map_step)

    return generate_frequency_map_cluster
