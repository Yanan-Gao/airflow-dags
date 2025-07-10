"""
IP Scan job (Azure only)

Job Details:
    - Results are read and fed downstream by the main ReceivedCounts (ColdStorageScan) DAG
    - Runs every 3 hours
    - Expected to run in <1 hour
    - Can only run one job at a time
"""
from datetime import datetime

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDatasources
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_environment = TtdEnvFactory.get_from_system()
cadence_in_hours = 3
dag_name = 'ip-scan'

# Prod variables
schedule = f'0 */{cadence_in_hours} * * *'
el_dorado_jar_url = constants.HPC_AZURE_EL_DORADO_JAR_URL

# Test variables
# schedule = None
# el_dorado_jar_url = 'abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/eldorado/mergerequests/kcc-HPC-4763-migrate-ip-scan/latest/eldorado-hpc-assembly.jar'

####################################################################################################################
# DAG
####################################################################################################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 7, 7),
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/L4ACG',
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag

####################################################################################################################
# IP Scan
####################################################################################################################
ip_scan_class_name = 'com.thetradedesk.jobs.receivedcounts.ipscan.IPScan'
ip_scan_cluster_name = 'counts-ip-scan'

ip_scan_cluster = HDIClusterTask(
    name=ip_scan_cluster_name,
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D12_v2(),
        workernode_type=HDIInstanceTypes.Standard_D14_v2(),
        num_workernode=3,
        disks_per_node=1
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    extra_script_actions=constants.DOWNLOAD_AEROSPIKE_CERT_AZURE_CLUSTER_SCRIPT_ACTION,
    enable_openlineage=False,
    cluster_version=HDIClusterVersions.AzureHdiSpark33
)

aerospike_truststore_password = "{{ conn.aerospike_truststore_password.get_password() }}"
ip_scan_eldorado_config = [("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"), ('cadenceInHours', cadence_in_hours),
                           ('address', constants.VAD_HC_ADDRESS), ('namespace', 'ttd-ip'), ('set', 'IPAddress'), ('sampleMod', 100),
                           ('javax.net.ssl.trustStorePassword', aerospike_truststore_password),
                           ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'),
                           ("azure.key", "eastusttdlogs,ttdexportdata"), ("ttd.ds.default.storageProvider", "azure"),
                           ('openlineage.enable', 'false')]

default_spark_config_azure_options = [
    ("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true"),
    (
        'spark.executor.extraJavaOptions',
        f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
    )
]

ip_scan_task = HDIJobTask(
    name='ip_scan_task',
    class_name=ip_scan_class_name,
    cluster_specs=ip_scan_cluster.cluster_specs,
    eldorado_config_option_pairs_list=ip_scan_eldorado_config,
    additional_args_option_pairs_list=default_spark_config_azure_options,
    configure_cluster_automatically=True,
    command_line_arguments=['--version'],
    jar_path=el_dorado_jar_url
)
ip_scan_cluster.add_parallel_body_task(ip_scan_task)

# Copy the TargetingDataIpReceivedCountsDataSet non-restricted dataset from azure to s3
targeting_data_ip_received_counts_non_restricted_copy_task = DatasetTransferTask(
    name='targeting_data_ip_received_counts_non_restricted_copy_task',
    dataset=CountsDatasources.targeting_data_ip_received_counts_non_restricted,
    src_cloud_provider=CloudProviders.azure,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_ip_received_counts_non_restricted.get_partitioning_args(
        ds_date="{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}"
    ),
)

####################################################################################################################
# Dependencies
####################################################################################################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> ip_scan_cluster >> targeting_data_ip_received_counts_non_restricted_copy_task >> final_dag_check
