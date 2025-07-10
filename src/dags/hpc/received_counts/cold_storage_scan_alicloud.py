"""
Cold Storage Scan job (Alicloud version)

Job Details:
    - Runs every 6 hours
    - Expected to run in <1 hour
    - Can only run one job at a time
"""
from datetime import datetime

from airflow.operators.python import ShortCircuitOperator

import dags.hpc.constants as constants
from dags.hpc.cloud_storage_to_sql_db_task.cloud_storage_to_sql_db_task import \
    create_cloud_storage_to_sql_db_task_with_airflow_dataset
from dags.hpc.cloud_storage_to_sql_db_task.utils import CloudStorageToSqlColumn, ConsistencyCheckCondition
from dags.hpc.counts_datasources import CountsDatasources
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
import dags.hpc.utils as hpc_utils

####################################################################################################################
# General Variables
####################################################################################################################
cadence_in_hours = constants.RECEIVED_COUNTS_CADENCE_IN_HOURS
job_environment = TtdEnvFactory.get_from_system()

# Prod Variables
dag_name = 'cold-storage-scan-alicloud'
schedule = f'0 */{cadence_in_hours} * * *'
el_dorado_jar_url = constants.HPC_ALI_EL_DORADO_JAR_URL
push_to_sql_enabled = True if job_environment == TtdEnvFactory.prod else False

# # Test Variables
# dag_name = 'dev-kcc-cold-storage-scan-alicloud'
# schedule = None
# el_dorado_jar_url = "oss://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-4387-update-cold-storage-scan-alicloud-aerospike-client/latest/el-dorado-assembly.jar"
# push_to_sql_enabled = True

####################################################################################################################
# DAG
####################################################################################################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 7, 28),
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/L4ACG',
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag

####################################################################################################################
# Cold Storage Scan Cluster
####################################################################################################################
cold_storage_scan_cluster_name = 'hpc-counts-cold-storage-scan-cluster'

cold_storage_scan_cluster = AliCloudClusterTask(
    name=cold_storage_scan_cluster_name,
    master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_2X(
    )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_4X()).with_node_count(3).with_data_disk_count(1)
    .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

####################################################################################################################
# Cold Storage Scan Job
####################################################################################################################
cold_storage_scan_class_name = constants.COLD_STORAGE_SCAN_CLASS_NAME

cold_storage_scan_eldorado_config = [("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                     ('address', constants.CHINA_COLD_STORAGE_ADDRESS),
                                     ('namespace', constants.CHINA_COLD_STORAGE_NAMESPACE), ('tlsEnabled', 'false'), ('scanSpeed', '0'),
                                     ('mod', '100'), ('cloudProvider', 'alicloud'), ('ttd.ds.default.storageProvider', 'alicloud'),
                                     ('outputMetaData', 'false'), ('cadenceInHours', cadence_in_hours),
                                     ('forceReducedUpdateValuesRefresh', 'false')]

default_spark_config_options = [("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true")]

cold_storage_scan_task = AliCloudJobTask(
    name='cold_storage_scan_task',
    class_name=cold_storage_scan_class_name,
    cluster_spec=cold_storage_scan_cluster.cluster_specs,
    eldorado_config_option_pairs_list=cold_storage_scan_eldorado_config,
    additional_args_option_pairs_list=default_spark_config_options,
    jar_path=el_dorado_jar_url,
    configure_cluster_automatically=True,
    command_line_arguments=['--version']
)
cold_storage_scan_cluster.add_parallel_body_task(cold_storage_scan_task)

####################################################################################################################
# Copy Output Received Counts to AWS S3 for downstream use
####################################################################################################################
copy_received_counts_from_alicloud_to_s3_task = DatasetTransferTask(
    name="copy_received_counts_from_alicloud_to_s3",
    dataset=CountsDatasources.targeting_data_received_counts_alicloud,
    src_cloud_provider=CloudProviders.ali,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_received_counts_alicloud.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    )
)

copy_deleted_received_counts_from_alicloud_to_s3_task = DatasetTransferTask(
    name="copy_deleted_received_counts_from_alicloud_to_s3",
    dataset=CountsDatasources.targeting_data_deleted_received_counts_alicloud,
    src_cloud_provider=CloudProviders.ali,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_deleted_received_counts_alicloud.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
)

copy_received_counts_by_uiid_type_from_alicloud_to_s3_task = DatasetTransferTask(
    name="copy_received_counts_by_uiid_type_from_alicloud_to_s3",
    dataset=CountsDatasources.targeting_data_received_counts_by_uiid_type_alicloud,
    src_cloud_provider=CloudProviders.ali,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=CountsDatasources.targeting_data_received_counts_by_uiid_type_alicloud.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    )
)

####################################################################################################################
# Provisioning Push Enabled Check
####################################################################################################################
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

####################################################################################################################
# Push Received Counts By User ID Type To Provisioning
####################################################################################################################

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
    name="targetingdatareceivedcountsbyuiidtypealicloud_to_provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts_by_uiid_type_alicloud,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts_by_uiid_type_alicloud.get_partitioning_args(
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
    consistency_check_based_on_column_values=[ConsistencyCheckCondition("SourceCloud", "AliCloud")],
    task_retries=1,
)

####################################################################################################################
# Dependencies
####################################################################################################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> cold_storage_scan_cluster
cold_storage_scan_cluster >> copy_received_counts_from_alicloud_to_s3_task
cold_storage_scan_cluster >> copy_deleted_received_counts_from_alicloud_to_s3_task
cold_storage_scan_cluster >> copy_received_counts_by_uiid_type_from_alicloud_to_s3_task

copy_received_counts_by_uiid_type_from_alicloud_to_s3_task >> is_push_to_sql_enabled
is_push_to_sql_enabled >> push_received_counts_by_id_type_to_provisioning_task

copy_received_counts_from_alicloud_to_s3_task >> final_dag_check
copy_deleted_received_counts_from_alicloud_to_s3_task >> final_dag_check
push_received_counts_by_id_type_to_provisioning_task >> final_dag_check
