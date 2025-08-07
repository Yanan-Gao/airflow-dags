from datetime import datetime

import dags.hpc.constants as constants
from dags.hpc.activecounts.datacollection.targetingdatalookup.persons_households_data_collection_shared import \
    get_persons_households_booster_cluster, get_xd_expansion_cluster
from dags.hpc.utils import CrossDeviceLevel
from datasources.sources.xdgraph_datasources import XdGraphDatasources, XdGraphVendorDataName
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
dag_name = 'person-households-data-collection-azure'
cadence_in_hours = 24

# Prod Variables
job_start_date = datetime(2024, 7, 8, 1, 0)
schedule = '0 21 * * *'
azure_jar = constants.HPC_AZURE_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# azure_jar = "abfs://ttd-build-artefacts@ttdeldorado.dfs.core.windows.net/eldorado/mergerequests/sjh-HPC-6537-filter-ctv-expansion-activity/latest/eldorado-hpc-assembly.jar"

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/lYGYFw',
    max_active_runs=1,
    run_only_latest=False,
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag

person_household_data_collection_headnode_type = HDIInstanceTypes.Standard_D12_v2()
person_household_data_collection_workernode_type = HDIInstanceTypes.Standard_D32A_v4()
person_household_data_collection_num_workernode = 12
person_household_data_collection_disks_per_node = 1

###########################################
# Check Dependencies
###########################################

recency_operator_step = OpTask(
    op=DatasetRecencyOperator(
        dag=adag,
        datasets_input=[XdGraphDatasources.xdGraph(XdGraphVendorDataName.IAv2_Person)],
        cloud_provider=CloudProviders.azure,
        recency_start_date=datetime.today(),
        lookback_days=12,
        xcom_push=True
    )
)

xd_graph_partition = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + XdGraphVendorDataName.IAv2_Person + "').strftime('%Y-%m-%d') }}"

###########################################
# Steps
###########################################

# Person Households Data Collection Cluster

person_household_data_collection_cluster_name = 'counts-person-data-collection'

person_household_data_collection_cluster = HDIClusterTask(
    name=person_household_data_collection_cluster_name,
    vm_config=HDIVMConfig(
        headnode_type=person_household_data_collection_headnode_type,
        workernode_type=person_household_data_collection_workernode_type,
        num_workernode=person_household_data_collection_num_workernode,
        disks_per_node=person_household_data_collection_disks_per_node
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    extra_script_actions=constants.DOWNLOAD_AEROSPIKE_CERT_AZURE_CLUSTER_SCRIPT_ACTION,
    # at the time of adding this, openlineage seems to be causing an issue with creating clusters
    enable_openlineage=False,
    cluster_version=HDIClusterVersions.AzureHdiSpark33
)

# Person Households Data Collection Step

person_household_data_collection_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.targetingdatalookup.PersonsHouseholdsDataCollection'
person_household_data_collection_step_job_name = 'person-data-collection'

aerospike_truststore_password = "{{ conn.aerospike_truststore_password.get_password() }}"

persons_households_data_collection_step_el_dorado_config_options = [
    ('processingDateHour', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'), ('xdGraphDate', xd_graph_partition),
    ('runCadence', 'slow'), ("azure.key", "eastusttdlogs,ttdexportdata"), ('aerospikeAddress', constants.COLD_STORAGE_ADDRESS),
    ('redisHost', 'gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com'), ('redisPort', '6379'),
    ('javax.net.ssl.trustStorePassword', aerospike_truststore_password),
    ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'), ('isRestrictedDataPipeline', 'true'),
    ('globalQpsLimit', 100000), ('lookupPartitionNum', 1000), ('threadPoolSize', 128), ('bucketSizeSec', 5), ('maxResultQueueSize', 512),
    ('openlineage.enable', 'false')
]

persons_households_data_collection_step_spark_config_options = [
    ("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true"),
    (
        'spark.executor.extraJavaOptions',
        f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
    )
]

person_household_data_collection_step = HDIJobTask(
    name=person_household_data_collection_step_job_name,
    jar_path=azure_jar,
    class_name=person_household_data_collection_step_spark_class_name,
    eldorado_config_option_pairs_list=persons_households_data_collection_step_el_dorado_config_options,
    additional_args_option_pairs_list=persons_households_data_collection_step_spark_config_options,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    configure_cluster_automatically=True
)

person_household_data_collection_cluster.add_parallel_body_task(person_household_data_collection_step)

# Persons Households Booster Step

persons_households_booster_cluster = get_persons_households_booster_cluster(
    CloudProviders.azure, azure_jar, xd_graph_partition, cadence_in_hours
)

# Households Expansion Step

households_expansion_cluster = get_xd_expansion_cluster(
    CrossDeviceLevel.HOUSEHOLDEXPANDED, xd_graph_partition, CloudProviders.azure, azure_jar, cadence_in_hours
)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> recency_operator_step >> person_household_data_collection_cluster >> persons_households_booster_cluster >> households_expansion_cluster >> final_dag_check
