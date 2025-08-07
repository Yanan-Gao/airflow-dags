from datetime import timedelta

from dags.cmkt.elasticsearch.common import cmkt_spark_aws_executable_path, get_common_eldorado_config, \
    emr_additional_args_option_pairs_list
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from typing import Optional, List
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.slack.slack_groups import cmkt

_snapshot_generator_aws_class_name = "{{ dag_run.conf.get('class_name', 'jobs.cmkt.elasticsearch.SnapshotGeneratorJob') }}"


def new_snapshot_generator_job_task(snapshot: str, xcom_pull_task_id, **kwargs) -> EmrJobTask:
    task_name = f"snapshot-generator-{snapshot}"

    # Place command line arguments here so it is less affected by the python_formatting CI step
    command_line_arguments = []
    command_line_arguments.append("--action={{ task_instance.xcom_pull(task_ids='" + xcom_pull_task_id + "', key='action') }}")
    command_line_arguments.append("--snapshotType=" + snapshot)
    command_line_arguments.append(
        "{{'--outputKey='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id + "', key='outputKey') if '" + snapshot +
        "' != 'MerchantProductDetails'}}"
    )
    command_line_arguments.append(
        "{{'--fullShipmentPrefix='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='fullShipmentPrefix') if task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='action')=='GenerateFromFullShipment'}}"
    )
    command_line_arguments.append(
        "{{'--fullShipmentPrefix='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='fullShipmentPrefix') if task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='action')=='GenerateFromFullShipmentAndIncrementals'}}"
    )
    command_line_arguments.append(
        "{{'--previousSnapshotPrefix='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='previousSnapshotPrefix') if task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='action')=='GenerateFromPreviousSnapshotAndIncrementals'}}"
    )
    command_line_arguments.append(
        "{{'--incrementalShipmentPrefixes='+','.join(task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='incrementalShipmentPrefixes')) if task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='action')=='GenerateFromPreviousSnapshotAndIncrementals'}}"
    )
    command_line_arguments.append(
        "{{'--incrementalShipmentPrefixes='+','.join(task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='incrementalShipmentPrefixes')) if task_instance.xcom_pull(task_ids='" + xcom_pull_task_id +
        "', key='action')=='GenerateFromFullShipmentAndIncrementals'}}"
    )
    command_line_arguments.append(
        "{{'--partitionedOutputKey='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id + "', key='partitionedOutputKey') if '" +
        snapshot + "'=='MerchantProduct'}}"
    )
    command_line_arguments.append(
        "{{'--partitionedOutputKey='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id + "', key='partitionedOutputKey') if '" +
        snapshot + "'=='MerchantProductDetails'}}"
    )
    command_line_arguments.append(
        "{{'--snapshotPrefixes='+task_instance.xcom_pull(task_ids='" + xcom_pull_task_id + "', key='snapshotPrefixes') if '" + snapshot +
        "' == 'MerchantProductDetails'}}"
    )

    pipeline_step = EmrJobTask(
        name=task_name,
        command_line_arguments=command_line_arguments,
        executable_path=cmkt_spark_aws_executable_path,
        class_name=_snapshot_generator_aws_class_name,
        configure_cluster_automatically=True,
        #   TODO: (CMKT-6628) when we have Azure, we can pass in CloudProvider.Azure
        eldorado_config_option_pairs_list=get_common_eldorado_config(),
        # source: dataproc/monitoring/spark_canary_job.py
        cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        additional_args_option_pairs_list=emr_additional_args_option_pairs_list,
        timeout_timedelta=timedelta(hours=3),
        retries=0,
    )

    #   TODO: (CMKT-6629)
    #   if cloud_provider == CloudProviders.azure:
    #    pipeline_step = HDIJobTask(
    #        name=task_name,
    #        # command_line_arguments=[
    #        #     "partition",
    #        # ],
    #        jar_path=azure_jar_path,
    #        class_name=class_name,
    #        configure_cluster_automatically=True,
    #        eldorado_config_option_pairs_list=common_eldorado_config + [
    #            # source: dataproc/monitoring/spark_canary_job.py
    #            ('openlineage.enable', 'false')
    #        ],
    #        additional_args_option_pairs_list=additional_args,
    #    )

    return pipeline_step


def new_cluster_task(
    name: str, core_instance_types: Optional[List[EmrInstanceType]] = None, weighted_capacity: int = None
) -> EmrClusterTask:
    cluster_tags = {"Team": cmkt.jira_team}

    etl_cluster = EmrClusterTask(
        name=f"run-cluster-{name}",
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=core_instance_types or [
                (M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)),
            ],
            on_demand_weighted_capacity=weighted_capacity or 4,
        ),
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        cluster_auto_termination_idle_timeout_seconds=60,
        cluster_tags=cluster_tags,
        retries=0,
    )

    #    TODO: (CMKT-6629)
    #    if cloud_provider == CloudProviders.azure:
    #        etl_cluster = HDIClusterTask(
    #            name=name,
    #            vm_config=HDIVMConfig(
    #                headnode_type=HDIInstanceTypes.Standard_D8A_v4(),
    #                workernode_type=HDIInstanceTypes.Standard_D8A_v4(),
    #                num_workernode=1,
    #            ),
    #            cluster_tags=cluster_tags,
    #            permanent_cluster=False,
    #            cluster_version=HDIClusterVersions.AzureHdiSpark31,
    #            enable_openlineage=False,
    #        )

    return etl_cluster
