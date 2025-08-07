from datetime import timedelta
from typing import Optional, List, Tuple

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.slack.slack_groups import pdg


def spark_fully_qualified_name(jar_package: str, main_class: str) -> str:
    return f"{jar_package}.{main_class}"


def create_emr_cluster_task(
    task_name: str,
    job_name: str,
    jar: str,
    main_class: str,
    timeout: timedelta,
    master_fleet: EmrFleetInstanceTypes = None,
    core_fleet: EmrFleetInstanceTypes = None,
    jvm_args: Optional[List[Tuple[str, str]]] = None,
    spark_args: Optional[List[Tuple[str, str]]] = None,
    jar_args: Optional[List[str]] = None,
    configure_cluster_automatically: bool = True,
    scaling_policy: Optional[EmrClusterScalingProperties] = None,
    retries: int = 1,
    deploy_mode="cluster"  # Default as per `EmrJobTask`.
) -> EmrClusterTask:
    cluster_task = EmrClusterTask(
        name=f"{task_name}",
        cluster_tags={
            "Team": pdg.jira_team,
            "Job": job_name,
            "Task": task_name,
            "Version": jar.split("/")[-3],
        },
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
        master_fleet_instance_type_configs=master_fleet,
        core_fleet_instance_type_configs=core_fleet,
        managed_cluster_scaling_config=scaling_policy,
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        retries=retries
    )

    job_task = EmrJobTask(
        name="job",
        class_name=main_class,
        executable_path=jar,
        command_line_arguments=jar_args,
        additional_args_option_pairs_list=spark_args,
        eldorado_config_option_pairs_list=jvm_args,
        configure_cluster_automatically=configure_cluster_automatically,
        timeout_timedelta=timeout,
        retries=0,
        deploy_mode=deploy_mode
    )

    if cluster_task is not None:
        cluster_task.add_parallel_body_task(job_task)
    return cluster_task


def create_hdi_cluster_task(
    task_name: str,
    job_name: str,
    jar: str,
    main_class: str,
    timeout: timedelta,
    vm_config: HDIVMConfig = None,
    jvm_args: Optional[List[Tuple[str, str]]] = None,
    spark_args: Optional[List[Tuple[str, str]]] = None,
    jar_args: Optional[List[str]] = None,
    configure_cluster_automatically: bool = True,
    retries: int = 1
) -> EmrClusterTask:
    cluster_task = HDIClusterTask(
        name=f"{task_name}",
        cluster_tags={
            "Team": pdg.jira_team,
            "Job": job_name,
            "Task": task_name,
            "Version": jar.split("/")[-3],
        },
        cluster_version=HDIClusterVersions.HDInsight51,
        enable_openlineage=False,  # Not supported for Spark 3 on HDI
        vm_config=vm_config,
        retries=retries
    )

    job_task = HDIJobTask(
        name="job",
        class_name=main_class,
        jar_path=jar,
        command_line_arguments=jar_args,
        additional_args_option_pairs_list=spark_args,
        eldorado_config_option_pairs_list=jvm_args,
        configure_cluster_automatically=configure_cluster_automatically,
        watch_step_timeout=timeout,
        cluster_specs=cluster_task.cluster_specs
    )

    if cluster_task is not None:
        cluster_task.add_parallel_body_task(job_task)
    return cluster_task


def scale_emr_cluster_up_to(max_capacity: int) -> EmrClusterScalingProperties:
    return EmrClusterScalingProperties(
        maximum_capacity_units=max_capacity,
        minimum_capacity_units=1,
        maximum_core_capacity_units=max_capacity,
        maximum_on_demand_capacity_units=max_capacity
    )
