from datetime import datetime, timedelta
from airflow import DAG
import dags.hpc.constants as constants
from dags.hpc.activecounts.countsredesign.reusable_dag_steps import get_dataset_check_sensor_task, \
    get_rotate_generation_ring_cluster
from dags.hpc.counts_datasources import CountsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from dags.hpc.utils import CrossDeviceLevel

###
# Variables
###
# General Variables
cadence_in_hours = 24  # Must match 'person-households-data-collection'.
schedule = '0 21 * * *'  # Must match 'person-households-data-collection'.

# Prod Variables
start_date = datetime(2024, 12, 25, 21, 0)
run_only_latest = True
end_date = None
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL
azr_jar = constants.HPC_AZURE_EL_DORADO_JAR_URL
cardinality_service_host = constants.CARDINALITY_SERVICE_PROD_HOST
cardinality_service_port = constants.CARDINALITY_SERVICE_PROD_PORT

# # Test Variables
# start_date = datetime(2024, 12, 25, 21, 0)
# run_only_latest = False
# end_date = start_date
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/sjh-HPC-6912-add-metrics-rotategenerationring/latest/eldorado-hpc-assembly.jar"
# azr_jar = "abfs://ttd-build-artefacts@ttdeldorado.dfs.core.windows.net/eldorado/mergerequests/dgs-HPC-6391-fix-deadline/latest/eldorado-hpc-assembly.jar"
# cardinality_service_host = constants.CARDINALITY_SERVICE_PROD_HOST
# cardinality_service_port = constants.CARDINALITY_SERVICE_PROD_PORT


###
# Helpers
###
# TODO (HPC-6323): This step assumes that the Persons/Households Data Collection Dataset contains the same Group Ids. However, this may not be the case and HPC-6323 should be done to fix it.
def get_active_group_ids_cluster(cross_device_level: CrossDeviceLevel) -> EmrClusterTask:
    """Gets a Cluster that sets Active Group Ids."""

    # Create cluster.
    set_active_group_ids_cluster = EmrClusterTask(
        name='set-active-group-ids',
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                M5.m5_2xlarge().with_fleet_weighted_capacity(1),
            ],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                M5.m5_2xlarge().with_fleet_weighted_capacity(1),
                M5.m5_4xlarge().with_fleet_weighted_capacity(2),
            ],
            on_demand_weighted_capacity=4,
        ),
        cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
        custom_java_version=17,
        region_name="us-east-1",
        retries=0
    )

    # Create task.
    eldorado_config_option_pairs_list = [('groupIdGenerationDateHour', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port),
                                         ('crossDeviceLevel', str(cross_device_level)), ("storageProvider", str(CloudProviders.aws))]
    additional_args_option_pairs_list = [('conf', 'spark.yarn.maxAppAttempts=1')]
    set_active_group_ids_task = EmrJobTask(
        name="set-active-group-ids",
        executable_path=aws_jar,
        class_name="com.thetradedesk.jobs.activecounts.countsredesign.setactivegroupids.SetActiveGroupIds",
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
        timeout_timedelta=timedelta(hours=3),
        configure_cluster_automatically=True,
    )
    set_active_group_ids_cluster.add_sequential_body_task(set_active_group_ids_task)

    return set_active_group_ids_cluster


def get_active_group_ids_transfer_task(cross_device_level: CrossDeviceLevel) -> DatasetTransferTask:
    """Transfers the Mapping of Group IDs to Global IDs from Aws to Azure."""

    # Note: This dataset is small (< 500MB) and the copy is cheap.
    dataset = CountsDatasources.get_id_to_global_id_map_dataset(cross_device_level)
    return DatasetTransferTask(
        name="copy-ids-to-global-ids-dataset",
        dataset=dataset,
        src_cloud_provider=CloudProviders.aws,
        dst_cloud_provider=CloudProviders.azure,
        partitioning_args=dataset.get_partitioning_args(ds_date="{{ logical_date.to_datetime_string() }}"),
        prepare_finalise_timeout=timedelta(minutes=30),
        drop_dst=True,
    )


def get_generate_group_bitmaps_cluster(
    cross_device_level: CrossDeviceLevel, cloud_provider: CloudProvider
) -> EmrClusterTask | HDIClusterTask:
    """Gets a cluster that sends GroupIds per Segment to Cardinality Service."""

    # Common
    timeout = timedelta(hours=3)
    cluster_name = f'{str(cloud_provider)}-{str(cross_device_level)}-generate-bitmaps'
    job_name = "com.thetradedesk.jobs.activecounts.countsredesign.generategroupbitmaps.GenerateGroupBitmaps"
    task_name = f"{str(cloud_provider)}-{str(cross_device_level)}-refresh-task"
    eldorado_config_option_pairs_list = [('groupIdGenerationDateHour', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port),
                                         ('crossDeviceLevel', str(cross_device_level)), ("ttd.ds.IdToGlobalIdMapDataSet.isInChain", "true"),
                                         ("storageProvider", str(cloud_provider)), ('openlineage.enable', 'false')]
    additional_args_option_pairs_list = [("conf", "spark.sql.shuffle.partitions=16384"),
                                         ("conf", "spark.sql.files.maxPartitionBytes=33554432"), ('conf', 'spark.yarn.maxAppAttempts=1')]

    # Create cluster.
    if cloud_provider == CloudProviders.aws:
        # Cluster.
        cluster = EmrClusterTask(
            name=cluster_name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
                                R7gd.r7gd_2xlarge().with_fleet_weighted_capacity(1)],
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    R5d.r5d_8xlarge().with_fleet_weighted_capacity(1),
                    R7gd.r7gd_8xlarge().with_fleet_weighted_capacity(1),
                    R5d.r5d_16xlarge().with_fleet_weighted_capacity(2),
                    R7gd.r7gd_16xlarge().with_fleet_weighted_capacity(2),
                    R5d.r5d_24xlarge().with_fleet_weighted_capacity(3),
                ],
                on_demand_weighted_capacity=15,
            ),
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
            custom_java_version=17,
            retries=0
        )

        # Task.
        task = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=job_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            timeout_timedelta=timeout,
            configure_cluster_automatically=True,
        )
        cluster.add_sequential_body_task(task)

        return cluster
    elif cloud_provider == CloudProviders.azure:
        # Cluster.
        cluster = HDIClusterTask(
            name=cluster_name,
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_openlineage=False,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            retries=3,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E32_v3(),
                workernode_type=HDIInstanceTypes.Standard_E32_v3(),
                num_workernode=15,  # TODO (HPC-6339): Further reduce cluster size to save on costs.
                disks_per_node=1
            ),
        )

        # Task.
        task = HDIJobTask(
            name=task_name,
            jar_path=azr_jar,
            class_name=job_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            watch_step_timeout=timeout,
            configure_cluster_automatically=True,
        )
        cluster.add_sequential_body_task(task)

        return cluster
    else:
        raise NotImplementedError(f"Job is not supported for {str(cloud_provider)}.")


def get_send_group_bitmaps_cluster(cross_device_level: CrossDeviceLevel, cloud_provider: CloudProvider):
    """Sends Bitmaps to Cardinality Service."""

    # Common
    timeout = timedelta(hours=3)
    cluster_name = f'{str(cloud_provider)}-{str(cross_device_level)}-send-bitmaps'
    job_name = "com.thetradedesk.jobs.activecounts.countsredesign.sendbitmaps.SendBitmaps"
    task_name = f"{str(cloud_provider)}-{str(cross_device_level)}-send-bitmaps-task"
    eldorado_config_option_pairs_list = [('groupIdGenerationDateHour', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port),
                                         ('crossDeviceLevel', str(cross_device_level)), ("ttd.ds.TargetingDataIdBitmaps.isInChain", "true"),
                                         ("storageProvider", str(cloud_provider)), ('openlineage.enable', 'false')]
    # TODO (HPC-6422): Speculation was enabled to temporarily resolve HPC-6422. Disable once a root cause is found.
    additional_args_option_pairs_list = [('conf', 'spark.yarn.maxAppAttempts=1'), ('conf', 'spark.speculation=true'),
                                         ('conf', 'spark.speculation.multiplier=5'), ('conf', 'spark.speculation.quantile=0.9')]
    # Create cluster.
    if cloud_provider == CloudProviders.aws:
        # Cluster.
        cluster = EmrClusterTask(
            name=cluster_name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M5.m5_2xlarge().with_fleet_weighted_capacity(1)],
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    M5.m5_4xlarge().with_fleet_weighted_capacity(1),
                    M5.m5_8xlarge().with_fleet_weighted_capacity(2),
                ],
                on_demand_weighted_capacity=2,
            ),
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
            custom_java_version=17,
            retries=0
        )

        # Task.
        task = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=job_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            timeout_timedelta=timeout,
            configure_cluster_automatically=True
        )
        cluster.add_sequential_body_task(task)

        return cluster
    elif cloud_provider == CloudProviders.azure:
        # Cluster.
        cluster = HDIClusterTask(
            name=cluster_name,
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_openlineage=False,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            retries=3,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_D8A_v4(),
                workernode_type=HDIInstanceTypes.Standard_D32A_v4(),
                num_workernode=1,
                disks_per_node=1
            ),
        )

        # Task.
        task = HDIJobTask(
            name=task_name,
            jar_path=azr_jar,
            class_name=job_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            watch_step_timeout=timeout,
            configure_cluster_automatically=True,
        )
        cluster.add_sequential_body_task(task)

        return cluster
    else:
        raise NotImplementedError(f"Job is not supported for {str(cloud_provider)}.")


def get_dag(cross_device_level: CrossDeviceLevel) -> DAG:
    """Creates a DAG based on a Person/Household CrossDeviceLevel."""

    dag = TtdDag(
        dag_id=f"counts-{str(cross_device_level)}-refresh",
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule,
        run_only_latest=run_only_latest,
        slack_channel=hpc.alarm_channel,
        dag_tsg='https://thetradedesk.atlassian.net/wiki/x/RgXuFg',
        tags=[hpc.jira_team],
        retries=0
    )
    airflow_dag = dag.airflow_dag

    ###
    # Steps
    ###
    # Check PersonsHouseholdsBooster partition.
    # Note: The PersonsHouseholdsBoosterDataset requires PersonsHouseholdsDataExportDataset to be generated.
    #       Hence, its existence implies the existence of PersonsHouseholdsDataExportDataset.
    persons_households_booster_dataset = CountsDatasources.persons_households_booster
    aws_persons_households_booster_dataset_check_sensor = get_dataset_check_sensor_task(
        airflow_dag, persons_households_booster_dataset, CloudProviders.aws, timeout=9
    )
    azr_persons_households_booster_dataset_check_sensor = get_dataset_check_sensor_task(
        airflow_dag, persons_households_booster_dataset, CloudProviders.azure, timeout=9
    )

    # Set Active Group Ids.
    set_active_group_ids_cluster = get_active_group_ids_cluster(cross_device_level)

    # Transfer Group ID to Global ID mappings.
    group_id_to_global_id_transfer_task = get_active_group_ids_transfer_task(cross_device_level)

    # Generate Bitmaps.
    aws_generate_bitmaps_cluster = get_generate_group_bitmaps_cluster(cross_device_level, CloudProviders.aws)
    azr_generate_bitmaps_cluster = get_generate_group_bitmaps_cluster(cross_device_level, CloudProviders.azure)

    # Send Bitmaps.
    aws_send_bitmaps_cluster = get_send_group_bitmaps_cluster(cross_device_level, CloudProviders.aws)
    azr_send_bitmaps_cluster = get_send_group_bitmaps_cluster(cross_device_level, CloudProviders.azure)

    # Rotate Generation Ring.
    rotate_generation_ring_cluster = get_rotate_generation_ring_cluster(
        cadence_in_hours, cross_device_level, cardinality_service_host, cardinality_service_port, aws_jar
    )

    # Check DAG status.
    final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=airflow_dag))

    ###
    # Dependencies
    ###
    dag >> aws_persons_households_booster_dataset_check_sensor
    dag >> azr_persons_households_booster_dataset_check_sensor

    aws_persons_households_booster_dataset_check_sensor >> set_active_group_ids_cluster
    set_active_group_ids_cluster >> aws_generate_bitmaps_cluster
    aws_generate_bitmaps_cluster >> aws_send_bitmaps_cluster
    aws_send_bitmaps_cluster >> rotate_generation_ring_cluster

    azr_persons_households_booster_dataset_check_sensor >> set_active_group_ids_cluster
    set_active_group_ids_cluster >> group_id_to_global_id_transfer_task
    group_id_to_global_id_transfer_task >> azr_generate_bitmaps_cluster
    azr_generate_bitmaps_cluster >> azr_send_bitmaps_cluster
    azr_send_bitmaps_cluster >> rotate_generation_ring_cluster

    rotate_generation_ring_cluster >> final_dag_check
    return airflow_dag


###
# DAG
###
persons_dag = get_dag(CrossDeviceLevel.PERSON)
households_dag = get_dag(CrossDeviceLevel.HOUSEHOLD)
