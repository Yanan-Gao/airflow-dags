from datetime import timedelta

from dags.forecast.utils.cluster_settings_library import ClusterSettings
from dags.forecast.utils.root_locations_s3 import RootLocationS3
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.ttdenv import TtdEnvFactory


def get_cluster(
    date: str,
    avail_stream: str,
    id_type: str,
    jar_path: str,
    emr_release_label: str,
    timeout: int,
    class_name: str,
    mapping_types: str = None,
    additional_config: list = [],
    cluster_settings: ClusterSettings = ClusterSettings()
) -> EmrClusterTask:
    job_environment = TtdEnvFactory.get_from_system()
    log_uri = f"{RootLocationS3.FORECASTING_ROOT}/env={job_environment.dataset_write_env}/frequency-maps/{cluster_settings.name}/logs"
    job_name_parts = ['Fmap-generation', cluster_settings.name, avail_stream, mapping_types, id_type]
    filtered_parts = [part for part in job_name_parts if part is not None]
    job_name = '-'.join(filtered_parts)

    config_pair_list = [('targetDate', date), ('date', date), ('mappingTypes', mapping_types), ('availStream', avail_stream),
                        ('idType', id_type)] + additional_config

    spark_options_list = [("executor-memory", cluster_settings.spark_executor_memory), ("executor-cores", "30"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=200G"),
                          ("conf", "spark.driver.maxResultSize=6G"),
                          ("conf", f"spark.sql.shuffle.partitions={cluster_settings.partition_count}"),
                          ("conf", "spark.yarn.maxAppAttempts=1")]

    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=cluster_settings.instance_type_master.value,
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=cluster_settings.instance_type_core.value,
        on_demand_weighted_capacity=cluster_settings.weighted_capacity,
    )

    cluster = EmrClusterTask(
        name=job_name,
        log_uri=log_uri,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags={
            "Team": slack_groups.FORECAST.team.jira_team,
        },
        enable_prometheus_monitoring=True,
        emr_release_label=emr_release_label,
    )

    step = EmrJobTask(
        cluster_specs=cluster.cluster_specs,
        name=job_name,
        class_name=class_name,
        executable_path=jar_path,
        additional_args_option_pairs_list=spark_options_list,
        eldorado_config_option_pairs_list=config_pair_list,
        timeout_timedelta=timedelta(hours=timeout)
    )

    cluster.add_sequential_body_task(step)
    return cluster
