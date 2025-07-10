"""
Cold Storage Scan job (Azure version)

Job Details:
    - Runs every 6 hours
    - Expected to run in <4 hour
    - Can only run one job at a time
"""
from datetime import datetime

from airflow.operators.python import ShortCircuitOperator, PythonOperator

import dags.hpc.constants as constants
import dags.hpc.utils as hpc_utils
from dags.hpc.cloud_storage_to_sql_db_task.cloud_storage_to_sql_db_task import \
    create_cloud_storage_to_sql_db_task_with_airflow_dataset
from dags.hpc.cloud_storage_to_sql_db_task.utils import CloudStorageToSqlColumn, ConsistencyCheckCondition
from dags.hpc.counts_datasources import CountsDatasources
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.interop.logworkflow_callables import LogFileBatchToVerticaLoadCallable
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

####################################################################################################################
# General Variables
####################################################################################################################
cadence_in_hours = constants.RECEIVED_COUNTS_CADENCE_IN_HOURS
job_environment = TtdEnvFactory.get_from_system()

# Prod Variables
dag_name = 'cold-storage-scan-azure'
schedule = f'0 */{cadence_in_hours} * * *'
el_dorado_jar_url = constants.HPC_AZURE_EL_DORADO_JAR_URL
verticaload_enabled = True if job_environment == TtdEnvFactory.prod else False
push_to_sql_enabled = True if job_environment == TtdEnvFactory.prod else False

# Test Variables
# dag_name = 'dev-kcc-data-marketplace-cold-storage-scan-azure'
# schedule = None
# el_dorado_jar_url = 'abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/eldorado/mergerequests/kcc-HPC-3388-use-identitysource-in-cold-storage-scan/latest/el-dorado-assembly.jar'
# verticaload_enabled = True
# push_to_sql_enabled = True

####################################################################################################################
# DAG
####################################################################################################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 7, 8),
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/L4ACG',
    tags=[hpc.jira_team],
)
adag = dag.airflow_dag

####################################################################################################################
# Cold Storage Scan
####################################################################################################################
cold_storage_scan_class_name = constants.COLD_STORAGE_SCAN_CLASS_NAME
cold_storage_scan_cluster_name = 'counts-cold-storage-scan-cluster'

cold_storage_scan_cluster = HDIClusterTask(
    name=cold_storage_scan_cluster_name,
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D5_v2(),
        workernode_type=HDIInstanceTypes.Standard_D5_v2(),
        num_workernode=6,
        disks_per_node=1
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    extra_script_actions=constants.DOWNLOAD_AEROSPIKE_CERT_AZURE_CLUSTER_SCRIPT_ACTION,
    enable_openlineage=False,
    cluster_version=HDIClusterVersions.AzureHdiSpark33
)

aerospike_truststore_password = "{{ conn.aerospike_truststore_password.get_password() }}"
cold_storage_scan_eldorado_config = [("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                     ('address', constants.COLD_STORAGE_ADDRESS), ('namespace',
                                                                                   'ttd-coldstorage-onprem'), ('scanSpeed', '0'),
                                     ('mod', '100'), ('cloudProvider', 'azure'), ('restrictedDataBinCloudServiceId', '4'),
                                     ('javax.net.ssl.trustStorePassword', aerospike_truststore_password),
                                     ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'),
                                     ('ttd.ds.default.storageProvider', 'azure'), ('ttd.cluster-service', 'HDInsight'),
                                     ('outputMetaData', 'false'), ('ttd.ds.TargetingDataIntTypeIdDataSet.cloudprovider', 'azure'),
                                     ('azure.key', 'eastusttdlogs,ttdexportdata'), ('forceReducedUpdateValuesRefresh', 'false'),
                                     ('cadenceInHours', cadence_in_hours), ('desiredMaxParallelScansPerNode', 4),
                                     ('openlineage.enable', 'false')]

default_spark_config_options = [
    ("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true"),
    (
        'spark.executor.extraJavaOptions',
        f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
    )
]

cold_storage_scan_task = HDIJobTask(
    name='cold_storage_scan_task',
    class_name=cold_storage_scan_class_name,
    cluster_specs=cold_storage_scan_cluster.cluster_specs,
    eldorado_config_option_pairs_list=cold_storage_scan_eldorado_config,
    additional_args_option_pairs_list=default_spark_config_options,
    configure_cluster_automatically=True,
    command_line_arguments=['--version'],
    jar_path=el_dorado_jar_url,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
)
cold_storage_scan_cluster.add_sequential_body_task(cold_storage_scan_task)

####################################################################################################################
# Copy Output Received Counts to AWS S3 for downstream use
####################################################################################################################
copy_received_counts_from_azure_to_s3 = DatasetTransferTask(
    name='run_task_copy_received_counts_from_azure_to_s3',
    dataset=CountsDatasources.targeting_data_received_counts_azure,
    src_cloud_provider=CloudProviders.azure,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_received_counts_azure.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
)

copy_deleted_received_counts_from_azure_to_s3 = DatasetTransferTask(
    name='run_task_copy_deleted_received_counts_from_azure_to_s3',
    dataset=CountsDatasources.targeting_data_deleted_received_counts_azure,
    src_cloud_provider=CloudProviders.azure,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_deleted_received_counts_azure.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
)

copy_received_counts_by_uiid_type_from_azure_to_s3 = DatasetTransferTask(
    name="copy_received_counts_by_uiid_type_from_azure_to_s3",
    dataset=CountsDatasources.targeting_data_received_counts_by_uiid_type_azure,
    src_cloud_provider=CloudProviders.azure,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_received_counts_by_uiid_type_azure.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    )
)

####################################################################################################################
# Push to Aerospike
####################################################################################################################
push_received_counts_to_aerospike_step_spark_class_name = 'com.thetradedesk.jobs.receivedcounts.pushtoaerospikereceivedcounts.PushToAerospikeReceivedCounts'

push_received_counts_to_aerospike_step_el_dorado_config_options = [('datetime', '{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                                                   ('aerospikeAddress', constants.AWS_COUNTS_AEROSPIKE_ADDRESS),
                                                                   ('cloudProvider', 'azure'),
                                                                   ('ttd.TargetingDataReceivedCountsDataSet.isInChain', 'true'),
                                                                   ('ttd.TargetingDataReceivedCountsDeletedDataSet.isInChain', 'true'),
                                                                   ('openlineage.enable', 'false')]

push_received_counts_to_aerospike_step_spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                                             ("conf", "spark.driver.maxResultSize=16G"),
                                                             ("conf", "spark.yarn.maxAppAttempts=1"),
                                                             ("conf", "spark.scheduler.spark.scheduler.minRegisteredResourcesRatio=0.90"),
                                                             ("conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=10m"),
                                                             ("conf", "spark.network.timeout=3600s")]

push_received_counts_to_aerospike_task = HDIJobTask(
    name='received_counts_push_to_aerospike_task',
    class_name=push_received_counts_to_aerospike_step_spark_class_name,
    cluster_specs=cold_storage_scan_cluster.cluster_specs,
    eldorado_config_option_pairs_list=push_received_counts_to_aerospike_step_el_dorado_config_options,
    additional_args_option_pairs_list=push_received_counts_to_aerospike_step_spark_options_list,
    configure_cluster_automatically=True,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=2),
    command_line_arguments=['--version'],
    jar_path=el_dorado_jar_url
)
cold_storage_scan_cluster.add_sequential_body_task(push_received_counts_to_aerospike_task)

###########################################
#   Provisioning Push Enabled Check
###########################################
# Whether to run push to provisioning
is_push_to_sql_enabled = OpTask(
    op=ShortCircuitOperator(
        task_id='is_push_to_sql_enabled',
        python_callable=hpc_utils.is_push_to_sql_enabled,
        dag=adag,
        trigger_rule='none_failed',
        provide_context=True,
        op_kwargs={'push_to_sql_enabled': push_to_sql_enabled},
        ignore_downstream_trigger_rules=False,
    )
)

###########################################
#   Push Received Counts To Provisioning
###########################################

push_received_counts_to_provisioning_column_mapping = [
    CloudStorageToSqlColumn('TargetingDataId', 'TargetingDataId'),
    CloudStorageToSqlColumn('Uniques', 'Counts'),
    CloudStorageToSqlColumn('DaysUntilLargeExpiration', 'DaysUntilLargeExpiration'),
    CloudStorageToSqlColumn('LastUpdatedInColdStorageAt', 'LastUpdatedInColdStorageAt'),
    CloudStorageToSqlColumn('DateTimeUpdated', 'LastGeneratedAt')
]

push_received_counts_to_provisioning_task = create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name="targetingdatareceivedcountsazure_to_provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts_azure,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
    destination_database_name="Provisioning",
    destination_table_schema_name="dbo",
    destination_table_name="ReceivedCounts",
    column_mapping=push_received_counts_to_provisioning_column_mapping,
    insert_to_staging_table_timeout=600,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=1800,
    task_retries=1
)

###########################################
#   Push Received Counts By User ID Type To Provisioning
###########################################

push_received_counts_by_id_type_to_provisioning_column_mapping = [
    CloudStorageToSqlColumn("TargetingDataId", "TargetingDataId"),
    CloudStorageToSqlColumn("TdidReceivedCount", "TdidCount"),
    CloudStorageToSqlColumn("DaidReceivedCount", "DaidCount"),
    CloudStorageToSqlColumn("IpReceivedCount", "IpCount"),
    CloudStorageToSqlColumn("Uid2ReceivedCount", "Uid2Count"),
    CloudStorageToSqlColumn("EuidReceivedCount", "EuidCount"),
    CloudStorageToSqlColumn("IdlReceivedCount", "IdlCount"),
    CloudStorageToSqlColumn("McIdReceivedCount", "McIdCount"),
    CloudStorageToSqlColumn("ID5ReceivedCount", "ID5Count"),
    CloudStorageToSqlColumn("NetIdReceivedCount", "NetIdCount"),
    CloudStorageToSqlColumn("FirstIdReceivedCount", "FirstIdCount"),
    CloudStorageToSqlColumn("SourceCloud", "SourceCloud"),
]

push_received_counts_by_id_type_to_provisioning_task = create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name="targetingdatareceivedcountsbyuiidtypeazure_to_provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts_by_uiid_type_azure,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts_by_uiid_type_azure.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
    destination_database_name="Provisioning",
    destination_table_schema_name="dbo",
    destination_table_name="ReceivedCountsByUserIdType",
    column_mapping=push_received_counts_by_id_type_to_provisioning_column_mapping,
    insert_to_staging_table_timeout=600,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=1800,
    ensure_consistency_between_source_and_destination=True,
    consistency_check_based_on_column_values=[ConsistencyCheckCondition("SourceCloud", "Azure")],
    task_retries=1
)

###########################################
#   Push Received Counts To Targeting DB
###########################################

push_received_counts_to_targetingdb_column_mapping = [
    CloudStorageToSqlColumn('TargetingDataId', 'TargetingDataId'),
    CloudStorageToSqlColumn('Uniques', 'Counts'),
    CloudStorageToSqlColumn('DaysUntilLargeExpiration', 'DaysUntilLargeExpiration'),
    CloudStorageToSqlColumn('LastUpdatedInColdStorageAt', 'LastUpdatedInColdStorageAt'),
    CloudStorageToSqlColumn('DateTimeUpdated', 'LastGeneratedAt')
]

push_received_counts_to_targetingdb_task = create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name="targetingdatareceivedcountsazure_to_targetingdb",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts_azure,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
    destination_database_name="Targeting",
    destination_table_schema_name="dbo",
    destination_table_name="ReceivedCounts",
    column_mapping=push_received_counts_to_targetingdb_column_mapping,
    insert_to_staging_table_timeout=600,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=1800,
    task_retries=1
)

###########################################
#   Vertica load received Counts
###########################################
is_vertica_load_enabled = OpTask(
    op=ShortCircuitOperator(
        task_id='is_vertica_load_enabled',
        python_callable=hpc_utils.is_vertica_load_enabled,
        dag=adag,
        trigger_rule='none_failed',
        provide_context=True,
        op_kwargs={'verticaload_enabled': verticaload_enabled},
        ignore_downstream_trigger_rules=False,
    )
)

vertica_load_received_counts = OpTask(
    op=PythonOperator(
        dag=adag,
        python_callable=LogFileBatchToVerticaLoadCallable,
        provide_context=True,
        op_kwargs={
            'database': constants.LOGWORKFLOW_DB,
            'mssql_conn_id': constants.LOGWORKFLOW_CONNECTION if job_environment ==
            TtdEnvFactory.prod else constants.LOGWORKFLOW_SANDBOX_CONNECTION,
            'get_object_list': hpc_utils.get_vertica_load_object_list,
            's3_prefix': constants.RECEIVED_COUNTS_AZURE_WRITE_PATH,
            'log_type_id': 162,
            # matches LogType.ReceivedCounts in TTD/Common/Logging/TTD.Common.Logging.Structured/LogType.cs
            'vertica_load_sproc_arguments': {
                'verticaTableId':
                94,
                # matches LogWorkflowVerticaTable.TTD_HistoricalReceivedCounts in TTD/DB/Vertica/TTD.DB.Vertica.Model/VerticaTables.cs
                'lineCount':
                1,
                'verticaTableCopyVersionId':
                231343666,
                # to calculate this, call "ThreadSafeMD5.ToHash( <copyColumnListSQL> )" defined in adplatform C# code
                'copyColumnListSQL':
                'TargetingDataId;Uniques;UniquesLast30Days;ReducedUpdateUniquesLast30Days FILLER NUMERIC;DaysUntilLargeExpiration FILLER NUMERIC;LastUpdatedInColdStorageAt FILLER TIMESTAMP;DateTimeUpdated'
                # use FILLER to ignore the ReducedUpdateUniquesLast30Days, DaysUntilLargeExpiration and LastUpdatedInColdStorageAt columns
            },
            'datetime': '{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'
        },
        task_id="vertica_load_received_counts",
    )
)

####################################################################################################################
# Dependencies
####################################################################################################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> cold_storage_scan_cluster
cold_storage_scan_cluster >> copy_received_counts_from_azure_to_s3
cold_storage_scan_cluster >> copy_deleted_received_counts_from_azure_to_s3
cold_storage_scan_cluster >> copy_received_counts_by_uiid_type_from_azure_to_s3

copy_received_counts_from_azure_to_s3 >> is_vertica_load_enabled
is_vertica_load_enabled >> vertica_load_received_counts >> final_dag_check

copy_received_counts_from_azure_to_s3 >> is_push_to_sql_enabled
copy_received_counts_by_uiid_type_from_azure_to_s3 >> is_push_to_sql_enabled
is_push_to_sql_enabled >> push_received_counts_to_provisioning_task
is_push_to_sql_enabled >> push_received_counts_by_id_type_to_provisioning_task
is_push_to_sql_enabled >> push_received_counts_to_targetingdb_task
push_received_counts_to_provisioning_task >> final_dag_check
push_received_counts_by_id_type_to_provisioning_task >> final_dag_check
push_received_counts_to_targetingdb_task >> final_dag_check
