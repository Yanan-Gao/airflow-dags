from datetime import datetime, timedelta

from dags.hpc import constants
from dags.hpc.counts_datasources import CountsDatasources
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

###########################################
# General Variables
###########################################
dag_name = 'ip-data-collection'
dag_start_date = datetime(2024, 7, 17)

# Prod variables
dag_schedule = timedelta(hours=3)
eldorado_jar_path = constants.HPC_AZURE_EL_DORADO_JAR_URL

# Test variables
# dag_schedule = None
# eldorado_jar_path = 'abfs://ttd-build-artefacts@ttdeldorado.dfs.core.windows.net/eldorado/mergerequests/kcc-HPC-6529-remove-coalesce-to-one-file/latest/eldorado-hpc-assembly.jar'

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=dag_start_date,
    schedule_interval=dag_schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/P4ABG',
    tags=[hpc.jira_team],
    run_only_latest=True
)

adag = dag.airflow_dag

###########################################
# IP Data Collection
###########################################
ip_data_collection_job_name = 'ip-data-collection'
ip_data_collection_cluster_name = f'{ip_data_collection_job_name}-cluster'
ip_data_collection_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.targetingdatalookup.ip.IPDataCollection'

ip_data_collection_cluster_task = HDIClusterTask(
    name=ip_data_collection_cluster_name,
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D12_v2(),
        workernode_type=HDIInstanceTypes.Standard_A8_v2(),
        num_workernode=4,
        disks_per_node=1
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_openlineage=False,
    cluster_version=HDIClusterVersions.AzureHdiSpark33
)

ip_data_collection_el_dorado_config_options = [('datetime', '{{ dag_run.start_date.strftime("%Y-%m-%dT%H:00:00") }}'),
                                               ('namespace', 'ttd-ip'), ('set', 'IPAddress'), ('address', constants.VAD_HC_ADDRESS),
                                               ('redisHost', 'gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com'),
                                               ('redisPort', '6379'), ('globalQpsLimit', 100000), ('threadPoolSize', 128),
                                               ('bucketSizeSec', 5), ("azure.key", "eastusttdlogs,ttdexportdata"),
                                               ("ttd.ds.default.storageProvider", "azure"),
                                               ("ttd.ds.ActiveIPAddressDataSet.prod.azureroot", f"{constants.TTD_COUNTS_AZURE}prod"),
                                               ("ttd.ds.ActiveIPAddressDataSet.test.azureroot", f"{constants.TTD_COUNTS_AZURE}test"),
                                               ('openlineage.enable', 'false')]

ip_data_collection_additional_args_option_pairs_list = [("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true")]

ip_data_collection_job_task = HDIJobTask(
    name=ip_data_collection_job_name,
    class_name=ip_data_collection_spark_class_name,
    cluster_specs=ip_data_collection_cluster_task.cluster_specs,
    eldorado_config_option_pairs_list=ip_data_collection_el_dorado_config_options,
    additional_args_option_pairs_list=ip_data_collection_additional_args_option_pairs_list,
    jar_path=eldorado_jar_path,
    configure_cluster_automatically=True,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
)

ip_data_collection_cluster_task.add_parallel_body_task(ip_data_collection_job_task)

###########################################
# Copy from Azure to AWS
###########################################
copy_ip_segments_from_azure_to_aws = DatasetTransferTask(
    name='copy_ip_segments_from_azure_to_aws',
    dataset=CountsDatasources.targeting_data_ip_segments_non_restricted,
    src_cloud_provider=CloudProviders.azure,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_ip_segments_non_restricted.get_partitioning_args(
        ds_date="{{ dag_run.start_date.strftime('%Y-%m-%d %H:00:00') }}"
    ),
)

copy_ip_dimension_targeting_data_from_azure_to_aws = DatasetTransferTask(
    name='copy_ip_dimension_targeting_data_from_azure_to_aws',
    dataset=CountsDatasources.ip_dimension_targeting_data,
    src_cloud_provider=CloudProviders.azure,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.ip_dimension_targeting_data.get_partitioning_args(
        ds_date="{{ dag_run.start_date.strftime('%Y-%m-%d %H:00:00') }}"
    ),
)

###########################################
# Dependencies
###########################################
final_dag_status_check_task = OpTask(op=FinalDagStatusCheckOperator(dag=adag))
dag >> ip_data_collection_cluster_task >> copy_ip_segments_from_azure_to_aws >> copy_ip_dimension_targeting_data_from_azure_to_aws >> final_dag_status_check_task
