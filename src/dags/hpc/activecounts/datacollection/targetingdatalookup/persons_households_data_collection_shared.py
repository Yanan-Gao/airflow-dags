from datetime import timedelta

import dags.hpc.constants as constants
from dags.hpc.utils import CrossDeviceLevel
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig


def get_persons_households_booster_cluster(
    cloud_provider: CloudProvider, jar: str, xd_graph_date: str, cadence_in_hours: int
) -> EmrClusterTask | HDIClusterTask:
    """Returns a cluster for the PersonsHouseholdsBooster step."""

    # Common.
    timeout_timedelta = timedelta(hours=6)
    cluster_name = 'persons-households-booster'
    task_name = 'persons-households-booster-task'
    class_name = "com.thetradedesk.jobs.activecounts.datacollection.personshouseholdsbooster.PersonsHouseholdsBooster"
    eldorado_config_option_pairs_list = [('aerospikeAddress', constants.COLD_STORAGE_ADDRESS),
                                         ('redisHost', 'gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com'),
                                         ('redisPort', '6379'),
                                         ('processingDateHour', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('xdGraphDate', xd_graph_date), ('cadenceInHours', cadence_in_hours),
                                         ("storageProvider", str(cloud_provider)),
                                         ("ttd.ds.PersonsHouseholdsDataExportDataSet.isInChain", "true")]
    additional_args_option_paris_list = [('conf', 'spark.yarn.maxAppAttempts=1')]

    if cloud_provider == CloudProviders.aws:
        # Cluster.
        cluster = EmrClusterTask(
            name=cluster_name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
                                R7gd.r7gd_2xlarge().with_fleet_weighted_capacity(1)],
                on_demand_weighted_capacity=1
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    R5d.r5d_8xlarge().with_fleet_weighted_capacity(1),
                    R5d.r5d_16xlarge().with_fleet_weighted_capacity(2),
                    R7gd.r7gd_8xlarge().with_fleet_weighted_capacity(1),
                    R7gd.r7gd_16xlarge().with_fleet_weighted_capacity(2)
                ],
                on_demand_weighted_capacity=10,
            ),
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
            retries=0,
        )

        # Task.
        task = EmrJobTask(
            name=task_name,
            executable_path=jar,
            class_name=class_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_paris_list,
            timeout_timedelta=timeout_timedelta,
            configure_cluster_automatically=True,
            cluster_calc_defaults=ClusterCalcDefaults(partitions=8192),
        )
        cluster.add_sequential_body_task(task)

        return cluster
    elif cloud_provider == CloudProviders.azure:
        # TODO (HPC-6562): Investigate why Azure runs take much longer.
        # Cluster.
        cluster = HDIClusterTask(
            name=cluster_name,
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_openlineage=False,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            extra_script_actions=constants.DOWNLOAD_AEROSPIKE_CERT_AZURE_CLUSTER_SCRIPT_ACTION,
            retries=3,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_E32_v3(),
                num_workernode=10,
                disks_per_node=1,
            )
        )

        # Task.
        trust_store = "/tmp/ttd-internal-root-ca-truststore.jks"
        trust_store_password = "{{ conn.aerospike_truststore_password.get_password() }}"
        azure_eldorado_config_option_pairs_list = [("azure.key", "eastusttdlogs,ttdexportdata"),
                                                   ('javax.net.ssl.trustStorePassword', trust_store_password),
                                                   ('javax.net.ssl.trustStore', trust_store), ('openlineage.enable', 'false')]
        azure_additional_args_option_pairs_list = [
            ("spark.sql.files.ignoreCorruptFiles", "true"), ("conf", "spark.sql.shuffle.partitions=8192"),
            (
                'spark.executor.extraJavaOptions',
                f'-Djavax.net.ssl.trustStorePassword={trust_store_password} -Djavax.net.ssl.trustStore={trust_store}'
            )
        ]
        task = HDIJobTask(
            name=task_name,
            jar_path=jar,
            class_name=class_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + azure_eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_paris_list + azure_additional_args_option_pairs_list,
            watch_step_timeout=timeout_timedelta,
            configure_cluster_automatically=True,
        )
        cluster.add_sequential_body_task(task)

        return cluster
    else:
        raise NotImplementedError(f"Job is not supported for {str(cloud_provider)}.")


# TODO (HPC-6323): This step assumes that the Persons/Households Data Collection Dataset contains the same Group Ids. However, this may not be the case and HPC-6323 should be done to fix it.
def get_xd_expansion_cluster(
    cross_device_level: CrossDeviceLevel, xd_graph_date: str, cloud_provider: CloudProvider, jar: str, cadence_in_hours: int
) -> EmrClusterTask | HDIClusterTask:
    """Gets a Cluster that expands HHs."""

    # Common
    timeout = timedelta(hours=3)
    cluster_name = f'{str(cloud_provider)}-{str(cross_device_level)}-expansion'
    job_class_name = "com.thetradedesk.jobs.activecounts.countsredesign.xdexpansion.expand.XdExpansion"
    task_name = f"{str(cloud_provider)}-{str(cross_device_level)}-expansion"
    eldorado_config_option_pairs_list = [('groupIdGenerationDateHour', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('xdGraphDate', xd_graph_date),
                                         ('crossDeviceLevel', str(cross_device_level)), ("storageProvider", str(cloud_provider)),
                                         ('openlineage.enable', 'false')]
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
                    R5.r5_16xlarge().with_fleet_weighted_capacity(16).with_ebs_size_gb(256),
                    R5.r5_24xlarge().with_fleet_weighted_capacity(24).with_ebs_size_gb(256),
                ],
                on_demand_weighted_capacity=512,
            ),
            cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4
        )

        # Create task.
        xd_expansion_task = EmrJobTask(
            name=task_name,
            executable_path=jar,
            class_name=job_class_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            timeout_timedelta=timedelta(hours=3),
            configure_cluster_automatically=True,
            cluster_calc_defaults=cluster_calc_defaults
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
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E32_v3(),
                workernode_type=HDIInstanceTypes.Standard_E32_v3(),
                num_workernode=15,
                disks_per_node=1
            ),
        )

        # Task.
        azure_eldorado_config_option_pairs_list = [("azure.key", "eastusttdlogs,ttdexportdata")]

        azure_additional_args_option_pairs_list = [("spark.sql.files.ignoreCorruptFiles", "true"),
                                                   ("conf", "spark.sql.shuffle.partitions=8192")]

        task = HDIJobTask(
            name=task_name,
            jar_path=jar,
            class_name=job_class_name,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + azure_eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list + azure_additional_args_option_pairs_list,
            watch_step_timeout=timeout,
            configure_cluster_automatically=True,
            cluster_calc_defaults=cluster_calc_defaults,
        )
        cluster.add_sequential_body_task(task)

        return cluster
    else:
        raise NotImplementedError(f"Job is not supported for {str(cloud_provider)}.")
