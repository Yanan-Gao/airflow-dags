from datetime import datetime

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDatasources, CountsDataName
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'devices-data-collection-azure'
cadence_in_hours = 24

# Prod Variables
job_start_date = datetime(2024, 7, 8, 1, 0)
schedule = '0 21 * * *'
azure_jar = constants.HPC_AZURE_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# azure_jar = 'abfs://ttd-build-artefacts@ttdeldorado.dfs.core.windows.net/eldorado/mergerequests/sjh-HPC-4907-move-person-households-optimisation/latest/eldorado-hpc-assembly.jar'

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/P4ABG',
    max_active_runs=1,
    run_only_latest=False,
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag

devices_data_collection_headnode_type = HDIInstanceTypes.Standard_D12_v2()
devices_data_collection_workernode_type = HDIInstanceTypes.Standard_D32A_v4()
devices_data_collection_num_workernode = 4
devices_data_collection_disks_per_node = 1

###########################################
# Check Dependencies
###########################################

recency_operator_step = OpTask(
    op=DatasetRecencyOperator(
        dag=adag,
        datasets_input=[CountsDatasources.get_counts_dataset(CountsDataName.ACTIVE_TDID_DAID)],
        cloud_provider=CloudProviders.azure,
        recency_start_date=datetime.today(),
        lookback_days=1,
        xcom_push=True
    )
)

avails_partition = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + CountsDataName.ACTIVE_TDID_DAID + "').strftime('%Y-%m-%dT%H:00:00') }}"

###########################################
# Steps
###########################################

# Devices Data Collection Cluster

devices_data_collection_cluster_name = 'counts-devices-data-collection'

devices_data_collection_cluster = HDIClusterTask(
    name=devices_data_collection_cluster_name,
    vm_config=HDIVMConfig(
        headnode_type=devices_data_collection_headnode_type,
        workernode_type=devices_data_collection_workernode_type,
        num_workernode=devices_data_collection_num_workernode,
        disks_per_node=devices_data_collection_disks_per_node
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    extra_script_actions=constants.DOWNLOAD_AEROSPIKE_CERT_AZURE_CLUSTER_SCRIPT_ACTION,
    # at the time of adding this, openlineage seems to be causing an issue with creating clusters
    enable_openlineage=False,
    cluster_version=HDIClusterVersions.AzureHdiSpark33
)

# Devices Data Collection Step

devices_data_collection_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.targetingdatalookup.DevicesDataCollection'
devices_data_collection_step_job_name = 'devices-data-collection'

aerospike_truststore_password = "{{ conn.aerospike_truststore_password.get_password() }}"

devices_data_collection_step_el_dorado_config_options = [
    ('availProcessingDateHour', avails_partition), ('aerospikeAddress', constants.COLD_STORAGE_ADDRESS),
    ('redisHost', 'gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com'), ('redisPort', '6379'),
    ('processingDateHour', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'), ('runCadence', 'slow'),
    ('ttd.ds.default.storageProvider', 'azure'), ('ttd.cluster-service', 'HDInsight'), ('openlineage.enable', 'false'),
    ('coldStorageReadPartitions', '15000'), ('isRestrictedDataPipeline', 'true'), ("ttd.ds.ActiveTdidDaidDataSet.cloudprovider", "azure"),
    ("ttd.ds.ActiveUidIdlDataSet.cloudprovider", "azure"),
    ("ttd.ds.ActiveTdidDaidDataSet.prod.azureroot", f"{constants.TTD_COUNTS_AZURE}prod"),
    ("ttd.ds.ActiveUidIdlDataSet.prod.azureroot", f"{constants.TTD_COUNTS_AZURE}prod"), ("azure.key", "eastusttdlogs,ttdexportdata"),
    ('javax.net.ssl.trustStorePassword', aerospike_truststore_password),
    ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'), ('globalQpsLimit', 100000), ('lookupPartitionNum', 1000),
    ('threadPoolSize', 128), ('bucketSizeSec', 5), ('maxResultQueueSize', 512)
]

devices_data_collection_step_spark_config_options = [
    ("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true"),
    (
        'spark.executor.extraJavaOptions',
        f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
    )
]

devices_data_collection_step = HDIJobTask(
    name=devices_data_collection_step_job_name,
    jar_path=azure_jar,
    class_name=devices_data_collection_step_spark_class_name,
    eldorado_config_option_pairs_list=devices_data_collection_step_el_dorado_config_options,
    additional_args_option_pairs_list=devices_data_collection_step_spark_config_options,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    configure_cluster_automatically=True
)

devices_data_collection_cluster.add_parallel_body_task(devices_data_collection_step)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> recency_operator_step >> devices_data_collection_cluster >> final_dag_check
