import logging
from datetime import timedelta, datetime

from airflow.utils import timezone
from typing import Dict, Optional, Sequence, Tuple, List

from dags.adpb.spark_jobs.shared.adpb_helper import xd_graph_lookback_days, \
    xd_graph_date_key_iav2, counts_aerospike_ttl, xd_graph_bucket, xd_graph_prefix_iav2
from datasources.sources.xdgraph_datasources import XdGraphDatasources, XdGraphVendorDataName
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder, CloudStorageBuilderException
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.tasks.chain import ChainOfTasks
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.hdinsight.script_action_spec import ScriptActionSpec
from ttd.ttdenv import TtdEnvFactory, TtdEnv

###
# Push to Aerospike functions with the Old and New ElDorado APIs
# - push_to_aerospike(): Uses the old API. Used in the slow and fast data processing subdags.
# - create_push_to_aerospike_task(): Uses the new API. Used in the full XD expansion subdag.
###

spark_timeout_delta = timedelta(hours=24)

standard_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

standard_core_fleet_instance_type_configs = [
    R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
    R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
    R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
]


class ClusterConfig:

    def __init__(
        self,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes = None,
        core_fleet_instance_type_configs: EmrFleetInstanceTypes = None,
        vm_config: HDIVMConfig = None,
        extra_script_actions: Optional[List[ScriptActionSpec]] = None
    ):
        self.master_fleet_instance_type_configs = master_fleet_instance_type_configs
        self.core_fleet_instance_type_configs = core_fleet_instance_type_configs
        self.vm_config = vm_config
        self.extra_script_actions = extra_script_actions


def get_cloud_provider(is_restricted_data_pipeline):
    return CloudProviders.aws if is_restricted_data_pipeline == 'false' else CloudProviders.azure


def get_public_certs_script_action():
    # for spark jobs running on azure that reads from cold storage
    # we need to copy the cert file onto the clusters with this shell script
    sas = "{{conn.azure_ttd_build_artefacts_sas.get_password()}}"
    url = "https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.0/latest/azure-scripts/ttd-internal-root-ca-truststore.jks"
    full_url = f"'{url}?{sas}'"
    return [
        ScriptActionSpec(
            action_name="download_public_certs",
            script_uri=
            "https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.0/latest/azure-scripts/download_from_url.sh",
            parameters=[full_url, "/tmp/ttd-internal-root-ca-truststore.jks"]
        )
    ]


def create_cluster(
    cloud_provider,
    cluster_name: str,
    cluster_tags: Dict[str, str],
    cluster_config: ClusterConfig,
    emr_release_label: str = None,
    environment: TtdEnv = TtdEnvFactory.get_from_system()
):
    if cloud_provider == CloudProviders.aws:
        return EmrClusterTask(
            name=cluster_name,
            master_fleet_instance_type_configs=cluster_config.master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=cluster_config.core_fleet_instance_type_configs,
            emr_release_label=emr_release_label,
            cluster_tags=cluster_tags,
            enable_prometheus_monitoring=True,
            use_on_demand_on_timeout=True,
            environment=environment
        )
    elif cloud_provider == CloudProviders.azure:
        return HDIClusterTask(
            name=cluster_name,
            vm_config=cluster_config.vm_config,
            cluster_tags=cluster_tags,
            environment=environment,
            extra_script_actions=cluster_config.extra_script_actions,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            # at the time of adding this, openlineage seems to be causing an issue with creating clusters
            enable_openlineage=False
        )
    else:
        raise Exception(f'Cloud Provider: {cloud_provider} is not supported.')


def create_job(
    cloud_provider,
    name: str,
    class_name: str,
    cluster_specs,
    eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
    azure_additional_eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
    additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
    azure_additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
    executable_path: str = None
):
    if cloud_provider == CloudProviders.aws:
        return EmrJobTask(
            name=name,
            class_name=class_name,
            executable_path=executable_path,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            timeout_timedelta=spark_timeout_delta,
            cluster_specs=cluster_specs,
            configure_cluster_automatically=True,
            command_line_arguments=['--version']
        )
    elif cloud_provider == CloudProviders.azure:
        additional_args_option = additional_args_option_pairs_list if azure_additional_args_option_pairs_list is None else azure_additional_args_option_pairs_list
        eldorado_config_option = eldorado_config_option_pairs_list if azure_additional_eldorado_config_option_pairs_list is None else eldorado_config_option_pairs_list + azure_additional_eldorado_config_option_pairs_list  # type: ignore
        return HDIJobTask(
            name=name,
            class_name=class_name,
            jar_path=executable_path,
            additional_args_option_pairs_list=additional_args_option,
            eldorado_config_option_pairs_list=eldorado_config_option,
            cluster_specs=cluster_specs,
            configure_cluster_automatically=True,
            command_line_arguments=['--version']
        )
    else:
        raise Exception(f'Cloud Provider: {cloud_provider} is not supported.')


def get_core_fleet_instance_type_configs(on_demand_capacity: int = 0):
    return EmrFleetInstanceTypes(
        instance_types=standard_core_fleet_instance_type_configs,
        on_demand_weighted_capacity=on_demand_capacity * 96,
    )


def convert_cloud_provider_from_string(cloud_provider_string):
    if cloud_provider_string == "aws":
        return CloudProviders.aws
    elif cloud_provider_string == "azure":
        return CloudProviders.azure
    elif cloud_provider_string == "alicloud":
        return CloudProviders.ali
    else:
        raise CloudStorageBuilderException(f'Cloud provider not supported - {cloud_provider_string}')


# check xd graph dependency
def get_xdgraph_date(**kwargs):
    cloud_storage = CloudStorageBuilder(convert_cloud_provider_from_string(kwargs['cloud_provider'])).build()
    job_date = datetime.strptime(kwargs.get('templates_dict').get('start_date'), "%Y-%m-%d")  # type: ignore
    check_recent_date = XdGraphDatasources.xdGraph(XdGraphVendorDataName.IAv2_Person
                                                   ).check_recent_data_exist(cloud_storage, job_date, xd_graph_lookback_days)
    if check_recent_date:
        # get the most recent date
        xd_graph_date_iav2 = check_recent_date.get().strftime('%Y-%m-%d')
        kwargs['task_instance'].xcom_push(key=xd_graph_date_key_iav2, value=xd_graph_date_iav2)
        logging.info(f'Found date: {xd_graph_date_iav2}')

    else:
        raise ValueError(f'Could not find xdGraph in last {xd_graph_lookback_days} days')


def push_to_aerospike(
    cluster_tags, el_dorado_jar_path, default_el_dorado_config_options, default_spark_config_options, counts_aerospike_namespace,
    counts_aerospike_address, counts_aerospike_address_with_tls, generation_str, is_restricted_data_pipeline, push_type_name,
    emr_release_label
):
    # pushTypeName must match the push job type name in the el-dorado job

    push_class_name = "com.thetradedesk.jobs.activecounts.hmhpipeline.publishtoaerospike.PublishToAerospike"
    if is_restricted_data_pipeline == "true":
        push_class_name = "com.thetradedesk.jobs.activecounts.hmhpipeline.publishtoaerospike.PublishToAerospikeWithTLS"

    push_cluster_name = f"hpc-{push_type_name}-cluster".lower()

    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=5,
    )

    cloud_provider = get_cloud_provider(is_restricted_data_pipeline)
    num_workernode = 8 if push_type_name == 'WriteContainmentIndexes' else 2
    cluster_config = ClusterConfig(
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        vm_config=HDIVMConfig(
            headnode_type=HDIInstanceTypes.Standard_D12_v2(),
            workernode_type=HDIInstanceTypes.Standard_A8_v2(),
            num_workernode=num_workernode,
            disks_per_node=1
        ),
        extra_script_actions=get_public_certs_script_action()
    )

    push_cluster = create_cluster(
        cloud_provider=cloud_provider,
        cluster_name=push_cluster_name,
        cluster_tags=cluster_tags,
        cluster_config=cluster_config,
        emr_release_label=emr_release_label
    )

    push_step_spark_options_list = default_spark_config_options + [
        ("executor-memory", "18G"),
        ("executor-cores", "5"),
        ("conf", "spark.driver.memory=18G"),
        ("conf", "spark.driver.cores=5"),
        ("conf", "spark.sql.shuffle.partitions=3000"),
        ("conf", "spark.scheduler.spark.scheduler.minRegisteredResourcesRatio=0.90"),
        ("conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=10m"),
        ("conf", "spark.network.timeout=3600s"),
        ("conf", "spark.default.parallelism=2800"),
    ]

    push_step_job_name = "TargetingDataCounts_PushToAerospike_" + push_type_name
    push_step_el_dorado_config_options = default_el_dorado_config_options + [
        ("jobType", push_type_name),
        ("aerospikeNamespace", counts_aerospike_namespace),
        ("aerospikeAddress", counts_aerospike_address),
        ("aerospikeAddressWithTLS", counts_aerospike_address_with_tls),
        ("ttl", counts_aerospike_ttl),
        ("generation", generation_str),
        ("isRestrictedDataPipeline", is_restricted_data_pipeline),
    ]

    # for spark jobs running on azure that reads from cold storage
    # we also need to specify the javax truststore properties to the spark driver and executor
    aerospike_truststore_password = "{{ conn.aerospike_truststore_password.get_password() }}"
    push_step_azure_additional_eldorado_config_options = [
        ('javax.net.ssl.trustStorePassword', aerospike_truststore_password),
        ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'),
    ]
    push_step_azure_spark_options_list = [
        ("spark.yarn.maxAppAttempts", 1),
        ("spark.sql.files.ignoreCorruptFiles", "true"),
        (
            'spark.executor.extraJavaOptions',
            f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
        ),
    ]

    push_step = create_job(
        cloud_provider=cloud_provider,
        name=push_step_job_name,
        class_name=push_class_name,
        cluster_specs=push_cluster.cluster_specs,
        eldorado_config_option_pairs_list=push_step_el_dorado_config_options,
        azure_additional_eldorado_config_option_pairs_list=push_step_azure_additional_eldorado_config_options,  # type: ignore
        additional_args_option_pairs_list=push_step_spark_options_list,
        azure_additional_args_option_pairs_list=push_step_azure_spark_options_list,  # type: ignore
        executable_path=el_dorado_jar_path
    )

    # Don't create PrepStep if we're doing TruncateAllSets
    if (is_restricted_data_pipeline == 'true') and (push_type_name != "TruncateAllSets"):

        push_prep_step_job_name = "TargetingDataCounts_PushToAerospike_" + push_type_name + "Prep"
        push_prep_step_el_dorado_config_options = default_el_dorado_config_options + [
            ("jobType", push_type_name + "Prep"),
            ("aerospikeNamespace", counts_aerospike_namespace),
            ("aerospikeAddress", counts_aerospike_address),
            ("aerospikeAddressWithTLS", counts_aerospike_address_with_tls),
            ("ttl", counts_aerospike_ttl),
            ("generation", generation_str),
            ("isRestrictedDataPipeline", is_restricted_data_pipeline),
        ]

        push_prep_step = create_job(
            cloud_provider=cloud_provider,
            name=push_prep_step_job_name,
            class_name=push_class_name,
            cluster_specs=push_cluster.cluster_specs,
            eldorado_config_option_pairs_list=push_prep_step_el_dorado_config_options,
            azure_additional_eldorado_config_option_pairs_list=push_step_azure_additional_eldorado_config_options,  # type: ignore
            additional_args_option_pairs_list=push_step_spark_options_list,
            azure_additional_args_option_pairs_list=push_step_azure_spark_options_list,  # type: ignore
            executable_path=el_dorado_jar_path
        )

        push_prep_step >> push_step

        push_cluster.add_parallel_body_task(ChainOfTasks(task_id="push-cluster-tasks", tasks=[push_prep_step, push_step]))

    else:
        push_cluster.add_parallel_body_task(push_step)

    return push_cluster, push_step


def create_push_to_aerospike_task(
    cluster_tags,
    is_restricted_data_pipeline,
    push_type_name,
    executable_path,
    spark_timeout_delta,
    default_el_dorado_config_options,
    counts_aerospike_namespace,
    counts_aerospike_address,
    counts_aersopike_address_tls,
    generation,
):
    """Pushes CIs/Sketches to Aersopike"""

    # Configs
    cluster_configs = {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.driver.memory": "18G",
            "spark.driver.cores": "5",
            "spark.sql.shuffle.partitions": "3000",
            "spark.scheduler.spark.scheduler.minRegisteredResourcesRatio": "0.90",
            "spark.scheduler.maxRegisteredResourcesWaitingTime": "10m",
            "spark.network.timeout": "3600s",
            "spark.default.parallelism": "2800",
        },
    }

    # Create Cluster Task
    cluster_task = EmrClusterTask(
        name=f"{push_type_name}_Aerospike",
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=5,
        ),
        emr_release_label="emr-5.36.0",
        cluster_tags=cluster_tags,
        enable_prometheus_monitoring=True,
        additional_application_configurations=[cluster_configs],
    )

    # Get Job Class
    class_name = "com.thetradedesk.jobs.activecounts.hmhpipeline.publishtoaerospike.PublishToAerospike"
    if is_restricted_data_pipeline == "true":
        class_name = "com.thetradedesk.jobs.activecounts.hmhpipeline.publishtoaerospike.PublishToAerospikeWithTLS"

    # Create Push Step Task
    push_step_el_dorado_configs = default_el_dorado_config_options + [
        ("jobType", push_type_name),
        ("aerospikeNamespace", counts_aerospike_namespace),
        ("aerospikeAddress", counts_aerospike_address),
        ("aerospikeAddressWithTLS", counts_aersopike_address_tls),
        ("ttl", counts_aerospike_ttl),
        ("generation", generation),
        ("isRestrictedDataPipeline", is_restricted_data_pipeline),
    ]

    push_step = EmrJobTask(
        name=f"TargetingDataCounts_PushToAerospike_{push_type_name}",
        class_name=class_name,
        executable_path=executable_path,
        timeout_timedelta=spark_timeout_delta,
        eldorado_config_option_pairs_list=push_step_el_dorado_configs,
        cluster_specs=cluster_task.cluster_specs,
    )

    # Don't create PrepStep if we're doing TruncateAllSets
    if (is_restricted_data_pipeline == "true") and (push_type_name != "TruncateAllSets"):

        # Create Prep Step Task
        prep_step_el_dorado_configs = default_el_dorado_config_options + [
            ("jobType", f"{push_type_name}Prep"),
            ("aerospikeNamespace", counts_aerospike_namespace),
            ("aerospikeAddress", counts_aerospike_address),
            ("aerospikeAddressWithTLS", counts_aersopike_address_tls),
            ("ttl", counts_aerospike_ttl),
            ("generation", generation),
            ("isRestrictedDataPipeline", is_restricted_data_pipeline),
        ]

        prep_step = EmrJobTask(
            name=f"TargetingDataCounts_PushToAerospike_{push_type_name}Prep",
            class_name=class_name,
            executable_path=executable_path,
            timeout_timedelta=spark_timeout_delta,
            eldorado_config_option_pairs_list=prep_step_el_dorado_configs,
            cluster_specs=cluster_task.cluster_specs,
        )

        # Dependencies
        prep_step >> push_step

        cluster_task.add_parallel_body_task(ChainOfTasks("prep-push-tasks", tasks=[prep_step, push_step]))

    else:
        cluster_task.add_parallel_body_task(push_step)

    return cluster_task


###
# Step to Truncate All Sets
###


def create_truncate_sets_step(
    cluster, is_restricted_data_pipeline, default_el_dorado_config_options, default_spark_config_options, counts_aerospike_namespace,
    counts_aerospike_address, counts_aerospike_address_with_tls, generation_str, el_dorado_jar_path, aerospike_truststore_password,
    cloud_provider
):
    """Tags a Step to a Cluster to Truncate All Aerospike Sets"""

    truncate_steps_job_name = 'TargetingDataCounts_TruncateAllSets'
    # Get Class Name
    truncate_steps_spark_class_name = 'com.thetradedesk.jobs.activecounts.hmhpipeline.publishtoaerospike.PublishToAerospike'
    if is_restricted_data_pipeline == 'true':
        truncate_steps_spark_class_name = 'com.thetradedesk.jobs.activecounts.hmhpipeline.publishtoaerospike.PublishToAerospikeWithTLS'

    # Configs
    truncate_step_el_dorado_config_options = default_el_dorado_config_options + [
        ('jobType', "TruncateAllSets"),
        ('aerospikeNamespace', counts_aerospike_namespace),
        ('aerospikeAddress', counts_aerospike_address),
        ('aerospikeAddressWithTLS', counts_aerospike_address_with_tls),
        ('ttl', counts_aerospike_ttl),
        ('generation', generation_str),
        ('isRestrictedDataPipeline', is_restricted_data_pipeline),
    ]
    # for spark jobs running on azure that reads from cold storage
    # we also need to specify the javax truststore properties to the spark driver and executor
    truncate_step_azure_additional_eldorado_config_options = [
        ('javax.net.ssl.trustStorePassword', aerospike_truststore_password),
        ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'),
    ]
    truncate_step_azure_spark_options_list = [
        ("spark.yarn.maxAppAttempts", 1),
        ("spark.sql.files.ignoreCorruptFiles", "true"),
        (
            'spark.executor.extraJavaOptions',
            f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
        ),
    ]
    truncate_sets_step = create_job(
        cloud_provider=cloud_provider,
        name=truncate_steps_job_name,
        class_name=truncate_steps_spark_class_name,
        cluster_specs=cluster.cluster_specs,
        eldorado_config_option_pairs_list=truncate_step_el_dorado_config_options,
        azure_additional_eldorado_config_option_pairs_list=truncate_step_azure_additional_eldorado_config_options,
        additional_args_option_pairs_list=default_spark_config_options,
        azure_additional_args_option_pairs_list=truncate_step_azure_spark_options_list,  # type: ignore
        executable_path=el_dorado_jar_path
    )

    return truncate_sets_step


def find_most_recent_dataset(start_date, lookback, bucket, path_prefix):
    hook = AwsCloudStorage(conn_id='aws_default')

    date = start_date
    for x in range(0, lookback):
        date_string = date.strftime("%Y-%m-%d")
        path = f'{path_prefix}{date_string}'
        logging.info(f'Checking {date}. Path: {path} ')

        if hook.check_for_prefix(bucket_name=bucket, delimiter='/', prefix=path):
            logging.info(f'{date_string} exists')
            break

        if x == (lookback - 1):
            raise ValueError(f'Could not find a {path_prefix} partition in the last {lookback} days')

        date = date - timedelta(days=1)
    return date.strftime('%Y-%m-%d')


def get_ready_for_spark_run(**kwargs):
    cur_time = timezone.utcnow()
    generation = int(cur_time.replace(tzinfo=timezone.utc).timestamp())

    kwargs['task_instance'].xcom_push(key='generation_str', value=str(generation))

    # Find most recent xd graph
    xd_graph_date_iav2 = find_most_recent_dataset(
        datetime.strptime(kwargs.get('templates_dict').get('start_date'), "%Y-%m-%d"),  # type: ignore
        xd_graph_lookback_days,
        xd_graph_bucket,  # type: ignore
        xd_graph_prefix_iav2
    )
    kwargs['task_instance'].xcom_push(key=xd_graph_date_key_iav2, value=xd_graph_date_iav2)
