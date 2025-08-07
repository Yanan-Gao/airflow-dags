"""
Cold Storage Scan job that outputs the received counts

Job Details:
    - Runs every 6 hours
    - Terminate in 12 hours if not finished
    - Expected to run in < 6 hour
    - Can only run one job at a time
"""
from datetime import datetime, timedelta

from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import dags.hpc.constants as constants
import dags.hpc.utils as hpc_utils
from dags.hpc.cloud_storage_to_sql_db_task.cloud_storage_to_sql_db_task import \
    create_cloud_storage_to_sql_db_task_with_airflow_dataset
from dags.hpc.cloud_storage_to_sql_db_task.utils import CloudStorageToSqlColumn, ConsistencyCheckCondition
from dags.hpc.counts_datasources import CountsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import LogFileBatchToVerticaLoadCallable
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

###########################################
# General Variables
###########################################
cadence_in_hours = constants.RECEIVED_COUNTS_CADENCE_IN_HOURS
job_environment = TtdEnvFactory.get_from_system()
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4

targeting_data_user_dag_name = "targeting-data-user"

dag_name = 'cold-storage-scan'

# Prod Variables
schedule = f'0 */{cadence_in_hours} * * *'
el_dorado_jar_url = constants.HPC_AWS_EL_DORADO_JAR_URL
verticaload_enabled = True if job_environment == TtdEnvFactory.prod else False
push_to_sql_enabled = True if job_environment == TtdEnvFactory.prod else False
meta_data_partition_date_string = '{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'

# Test Variables
# schedule = None
# el_dorado_jar_url = "s3://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-4284-migrate-received-counts-jobs-to-spark-3/latest/eldorado-hpc-assembly.jar"
# verticaload_enabled = True
# push_to_sql_enabled = True
# meta_data_partition_date_string = '2024-08-23T05:00:00'

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 8, 28),
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/L4ACG',
    tags=[hpc.jira_team],
)
adag = dag.airflow_dag

###########################################
# Cold Storage Scan
###########################################
cold_storage_scan_class_name = constants.COLD_STORAGE_SCAN_CLASS_NAME
cold_storage_scan_cluster_name = 'cold_storage_scan_cluster'

cold_storage_scan_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6g.r6g_8xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

cold_storage_scan_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_16xlarge().with_ebs_size_gb(4096).with_fleet_weighted_capacity(1),
    ], on_demand_weighted_capacity=24
)

cold_storage_scan_cluster = EmrClusterTask(
    name=cold_storage_scan_cluster_name,
    master_fleet_instance_type_configs=cold_storage_scan_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=cold_storage_scan_core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

cold_storage_scan_props = [('conf', 'spark.network.timeout=3600s'), ('conf', 'spark.yarn.max.executor.failures=1000'),
                           ('conf', 'spark.yarn.executor.failuresValidityInterval=1h'),
                           ('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC'), ('conf', 'spark.yarn.maxAppAttempts=1')]

cold_storage_scan_args = [
    ("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
    ('address', constants.COLD_STORAGE_ADDRESS),
    ('namespace', 'ttd-coldstorage-onprem'),
    ('scanSpeed', '0'),
    ('mod', '100'),
    ('cloudProvider', 'aws'),
    ('cadenceInHours', cadence_in_hours),
    ('ttd.ds.default.storageProvider', 'aws'),
    ('forceReducedUpdateValuesRefresh', 'false'),
    ('desiredMaxParallelScansPerNode', 2),
    (
        'outputMetaData',
        "{{ task_instance.xcom_pull(task_ids='is_targeting_data_user_trigger_enabled', key='output_meta_data', include_prior_dates=True, default='true') }}"
    ),
]

cold_storage_scan_task = EmrJobTask(
    cluster_specs=cold_storage_scan_cluster.cluster_specs,
    name='cold_storage_scan_task',
    class_name=cold_storage_scan_class_name,
    additional_args_option_pairs_list=cold_storage_scan_props,
    eldorado_config_option_pairs_list=cold_storage_scan_args,
    executable_path=el_dorado_jar_url,
    timeout_timedelta=timedelta(hours=12),
    configure_cluster_automatically=True
)

cold_storage_scan_cluster.add_parallel_body_task(cold_storage_scan_task)

###########################################
#   Push To Aerospike
###########################################
push_received_counts_to_aerospike_cluster_name = 'PushToAerospikeReceivedCounts_Cluster'
push_received_counts_to_aerospike_timeout = timedelta(hours=6)

push_received_counts_to_aerospike_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

push_received_counts_to_aerospike_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7g_4xlarge().with_fleet_weighted_capacity(4),
        M7g.m7g_8xlarge().with_fleet_weighted_capacity(8),
        M7g.m7g_12xlarge().with_fleet_weighted_capacity(12),
        M7g.m7g_16xlarge().with_fleet_weighted_capacity(16),
    ],
    on_demand_weighted_capacity=4 * 4
)

push_received_counts_to_aerospike_cluster = EmrClusterTask(
    name=push_received_counts_to_aerospike_cluster_name,
    master_fleet_instance_type_configs=push_received_counts_to_aerospike_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=push_received_counts_to_aerospike_core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

push_received_counts_to_aerospike_step_spark_class_name = 'com.thetradedesk.jobs.receivedcounts.pushtoaerospikereceivedcounts.PushToAerospikeReceivedCounts'
push_received_counts_to_aerospike_step_job_name = 'ReceivedCounts_PushToAerospike'
push_received_counts_to_aerospike_step_el_dorado_config_options = [
    ("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
    ('aerospikeAddress', constants.AWS_COUNTS_AEROSPIKE_ADDRESS),
    ('cloudProvider', 'aws'),
    ('ttd.TargetingDataReceivedCountsDataSet.isInChain', 'true'),
    ('ttd.TargetingDataReceivedCountsDeletedDataSet.isInChain', 'true'),
]

push_received_counts_to_aerospike_step_spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                                             ("conf", "spark.network.timeout=3600s")]

push_received_counts_to_aerospike_task = EmrJobTask(
    cluster_specs=push_received_counts_to_aerospike_cluster.cluster_specs,
    name=push_received_counts_to_aerospike_step_job_name,
    class_name=push_received_counts_to_aerospike_step_spark_class_name,
    additional_args_option_pairs_list=push_received_counts_to_aerospike_step_spark_options_list,
    eldorado_config_option_pairs_list=push_received_counts_to_aerospike_step_el_dorado_config_options,
    executable_path=el_dorado_jar_url,
    timeout_timedelta=push_received_counts_to_aerospike_timeout,
    configure_cluster_automatically=True
)

push_received_counts_to_aerospike_cluster.add_parallel_body_task(push_received_counts_to_aerospike_task)

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
    name="targetingdatareceivedcounts_to_provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
    destination_database_name="Provisioning",
    destination_table_schema_name="dbo",
    destination_table_name="ReceivedCounts",
    column_mapping=push_received_counts_to_provisioning_column_mapping,
    insert_to_staging_table_timeout=timedelta(hours=1).seconds,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=timedelta(hours=4).seconds,
    resources=TaskServicePodResources().custom(request_cpu="2", request_memory="16Gi", limit_memory="16Gi", limit_ephemeral_storage="2Gi"),
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
    name="targetingdatareceivedcountsbyuiidtype_to_provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts_by_uiid_type,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts_by_uiid_type.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
    destination_database_name="Provisioning",
    destination_table_schema_name="dbo",
    destination_table_name="ReceivedCountsByUserIdType",
    column_mapping=push_received_counts_by_id_type_to_provisioning_column_mapping,
    insert_to_staging_table_timeout=timedelta(hours=1).seconds,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=timedelta(hours=2).seconds,
    ensure_consistency_between_source_and_destination=True,
    consistency_check_based_on_column_values=[ConsistencyCheckCondition("SourceCloud", "Aws")],
    resources=TaskServicePodResources().custom(request_cpu="2", request_memory="8Gi", limit_memory="8Gi", limit_ephemeral_storage="1Gi"),
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
    name="targetingdatareceivedcounts_to_targetingdb",
    scrum_team=hpc,
    dataset=CountsDatasources.targeting_data_received_counts,
    dataset_partitioning_args=CountsDatasources.targeting_data_received_counts.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
    ),
    destination_database_name="Targeting",
    destination_table_schema_name="dbo",
    destination_table_name="ReceivedCounts",
    column_mapping=push_received_counts_to_targetingdb_column_mapping,
    insert_to_staging_table_timeout=timedelta(hours=1).seconds,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=timedelta(hours=4).seconds,
    resources=TaskServicePodResources().custom(request_cpu="2", request_memory="16Gi", limit_memory="16Gi", limit_ephemeral_storage="2Gi"),
    task_retries=1
)

###########################################
#   Vertica load received Counts
###########################################
# Whether to run VerticaLoad
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
            's3_prefix': constants.RECEIVED_COUNTS_WRITE_PATH,
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


###########################################
#   Trigger Targeting Data User DAG
###########################################
# To save money, we only output the meta data dataset and trigger the targeting data user dag every other run
def is_targeting_data_user_trigger_enabled(task_instance: TaskInstance):
    output_meta_data = task_instance.xcom_pull(
        task_ids='is_targeting_data_user_trigger_enabled', key='output_meta_data', include_prior_dates=True, default='true'
    )
    next_output_meta_data = 'false' if output_meta_data == 'true' else 'true'
    task_instance.xcom_push(key="output_meta_data", value=next_output_meta_data)
    return output_meta_data == 'true'


is_targeting_data_user_trigger_enabled = OpTask(
    op=ShortCircuitOperator(
        task_id='is_targeting_data_user_trigger_enabled',
        python_callable=is_targeting_data_user_trigger_enabled,
        dag=adag,
        trigger_rule='none_failed',
        ignore_downstream_trigger_rules=False,
    )
)

targeting_data_user_trigger = OpTask(
    op=TriggerDagRunOperator(
        task_id="trigger_targeting_data_user",
        trigger_dag_id=targeting_data_user_dag_name,
        conf={"metaDataPartitionDate": meta_data_partition_date_string},
        dag=adag,
    )
)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> cold_storage_scan_cluster
cold_storage_scan_cluster >> push_received_counts_to_aerospike_cluster
cold_storage_scan_cluster >> is_push_to_sql_enabled
is_push_to_sql_enabled >> push_received_counts_to_provisioning_task
is_push_to_sql_enabled >> push_received_counts_by_id_type_to_provisioning_task
is_push_to_sql_enabled >> push_received_counts_to_targetingdb_task
cold_storage_scan_cluster >> is_vertica_load_enabled >> vertica_load_received_counts
cold_storage_scan_cluster >> is_targeting_data_user_trigger_enabled >> targeting_data_user_trigger

cold_storage_scan_cluster >> final_dag_check
push_received_counts_to_aerospike_cluster >> final_dag_check
push_received_counts_to_provisioning_task >> final_dag_check
push_received_counts_by_id_type_to_provisioning_task >> final_dag_check
push_received_counts_to_targetingdb_task >> final_dag_check
vertica_load_received_counts >> final_dag_check
targeting_data_user_trigger >> final_dag_check
