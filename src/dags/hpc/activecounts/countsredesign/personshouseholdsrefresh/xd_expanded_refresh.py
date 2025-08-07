from datetime import datetime, timedelta

from airflow import DAG

import dags.hpc.constants as constants
from dags.hpc.activecounts.countsredesign.reusable_dag_steps import get_dataset_check_sensor_task, \
    get_rotate_generation_ring_cluster
from dags.hpc.utils import CrossDeviceLevel
from dags.hpc.counts_datasources import CountsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

###
# Variables
###
# General Variables
start_date = datetime(2025, 2, 3, 21, 0)
cadence_in_hours = 24  # Must match 'person-households-data-collection'.
schedule = '0 21 * * *'  # Must match 'person-households-data-collection'.

# Prod Variables
run_only_latest = True
end_date = None
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL
azr_jar = constants.HPC_AZURE_EL_DORADO_JAR_URL
cardinality_service_host = constants.CARDINALITY_SERVICE_PROD_HOST
cardinality_service_port = constants.CARDINALITY_SERVICE_PROD_PORT

# # Test Variables
# run_only_latest = False
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/sjh-HPC-6537-filter-ctv-expansion-activity/latest/eldorado-hpc-assembly.jar"
# azr_jar = "abfs://ttd-build-artefacts@ttdeldorado.dfs.core.windows.net/eldorado/mergerequests/sjh-HPC-6537-filter-ctv-expansion-activity/latest/eldorado-hpc-assembly.jar"
# cardinality_service_host = constants.CARDINALITY_SERVICE_TEST_HOST


def get_active_xd_expanded_ids_cluster(cross_device_level: CrossDeviceLevel) -> EmrClusterTask:
    """Gets a Cluster that sets Active Ids."""

    # Create cluster.
    set_active_xd_expanded_ids_cluster = EmrClusterTask(
        name='set-active-xd-expanded-ids-cluster',
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                M7g.m7g_2xlarge().with_fleet_weighted_capacity(1),
            ],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                M7g.m7g_2xlarge().with_fleet_weighted_capacity(2),
                M7g.m7g_4xlarge().with_fleet_weighted_capacity(4),
            ],
            on_demand_weighted_capacity=32,
        ),
        cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5_0,
        retries=0
    )

    # Create task.
    eldorado_config_option_pairs_list = [('datetime', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port),
                                         ('crossDeviceLevel', str(cross_device_level)), ("storageProvider", str(CloudProviders.aws)),
                                         ('ttd.DimensionTargetingDataDataSet.isInChain', 'true')]

    set_active_xd_expanded_ids_task = EmrJobTask(
        name="set-active-group-ids",
        executable_path=aws_jar,
        class_name="com.thetradedesk.jobs.activecounts.countsredesign.xdexpansion.refresh.SetXdExpandedActiveIds",
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
        timeout_timedelta=timedelta(hours=3),
        configure_cluster_automatically=True,
    )
    set_active_xd_expanded_ids_cluster.add_sequential_body_task(set_active_xd_expanded_ids_task)

    return set_active_xd_expanded_ids_cluster


def get_dimension_refresh(cross_device_level: CrossDeviceLevel, cloud_provider: CloudProvider) -> EmrClusterTask | HDIClusterTask:
    """Gets a Cluster that expands HHs."""

    # Common
    retries = 0
    timeout = timedelta(hours=6)
    cluster_name = f'{str(cloud_provider)}-{str(cross_device_level)}-refresh'
    job_class_name = "com.thetradedesk.jobs.activecounts.countsredesign.xdexpansion.refresh.XdExpandedRefresh"
    task_name = f"{str(cloud_provider)}-{str(cross_device_level)}-refresh"
    eldorado_config_option_pairs_list = [('datetime', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port),
                                         ('crossDeviceLevel', str(cross_device_level)), ("storageProvider", str(cloud_provider)),
                                         ('openlineage.enable', 'false'), ('ttd.DimensionTargetingDataDataSet.isInChain', 'true')]
    additional_args_option_pairs_list = [('conf', 'spark.yarn.maxAppAttempts=1')]
    cluster_calc_defaults = ClusterCalcDefaults(min_executor_memory=50, max_cores_executor=4, parallelism_factor=40)

    # Create cluster.
    if cloud_provider == CloudProviders.aws:
        # Create cluster.
        cluster = EmrClusterTask(
            name=cluster_name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    M5.m5_8xlarge().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    R5.r5_2xlarge().with_fleet_weighted_capacity(2),
                    R5.r5_4xlarge().with_fleet_weighted_capacity(4),
                ],
                on_demand_weighted_capacity=192,
            ),
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5_0,
            retries=retries
        )

        # Create task.
        xd_expansion_task = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=job_class_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            timeout_timedelta=timedelta(hours=3),
            configure_cluster_automatically=True,
            cluster_calc_defaults=cluster_calc_defaults,
        )
        cluster.add_sequential_body_task(xd_expansion_task)

        return cluster

    elif cloud_provider == CloudProviders.azure:
        # Cluster.
        cluster = HDIClusterTask(
            name=cluster_name,
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_openlineage=False,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            retries=retries,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E32_v3(),
                workernode_type=HDIInstanceTypes.Standard_E32_v3(),
                num_workernode=3,
                disks_per_node=1
            ),
        )

        # Task.
        task = HDIJobTask(
            name=task_name,
            jar_path=azr_jar,
            class_name=job_class_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + [("azure.key", "eastusttdlogs,ttdexportdata")],
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            watch_step_timeout=timeout,
            configure_cluster_automatically=True,
            cluster_calc_defaults=cluster_calc_defaults,
        )
        cluster.add_sequential_body_task(task)

        return cluster
    else:
        raise NotImplementedError(f"Job is not supported for {str(cloud_provider)}.")


def get_dag(cross_device_level: CrossDeviceLevel) -> DAG:
    """Creates a DAG based on a Person/Household Expanded CrossDeviceLevel."""

    dag = TtdDag(
        dag_id=f"counts-{str(cross_device_level)}-refresh",
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule,
        run_only_latest=run_only_latest,
        slack_channel=hpc.alarm_channel,
        dag_tsg='https://thetradedesk.atlassian.net/wiki/x/JgAyG',
        tags=[hpc.jira_team],
        retries=0
    )
    airflow_dag = dag.airflow_dag

    ###
    # Steps
    ###

    # Check existence of PersonsHouseholdsDataExportDataset partition.
    upstream_dataset = CountsDatasources.households_expansion
    aws_upstream_dataset_check_sensor = get_dataset_check_sensor_task(airflow_dag, upstream_dataset, CloudProviders.aws, timeout=9)
    azr_upstream_dataset_check_sensor = get_dataset_check_sensor_task(airflow_dag, upstream_dataset, CloudProviders.azure, timeout=9)

    # Set Next Generation
    set_next_generation = get_active_xd_expanded_ids_cluster(cross_device_level)

    # Set Dimensions
    aws_dimension_refresh = get_dimension_refresh(cross_device_level, CloudProviders.aws)
    azure_dimension_refresh = get_dimension_refresh(cross_device_level, CloudProviders.azure)

    # Rotate Ring
    rotate_generation_ring_cluster = get_rotate_generation_ring_cluster(
        cadence_in_hours, cross_device_level, cardinality_service_host, cardinality_service_port, aws_jar
    )

    # Check DAG status.
    final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=airflow_dag))

    ###
    # Dependencies
    ###
    dag >> aws_upstream_dataset_check_sensor >> set_next_generation >> aws_dimension_refresh >> rotate_generation_ring_cluster >> final_dag_check
    dag >> azr_upstream_dataset_check_sensor >> set_next_generation >> azure_dimension_refresh >> rotate_generation_ring_cluster >> final_dag_check

    return airflow_dag


###
# DAG
###
households_dag = get_dag(CrossDeviceLevel.HOUSEHOLDEXPANDED)
