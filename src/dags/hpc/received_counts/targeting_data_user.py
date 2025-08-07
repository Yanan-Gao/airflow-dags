"""
Produces the vertica targeting data user table updates from cold storage scan

Job Details:
    - Runs every 12 hours
    - Terminate in 12 hours if not finished
    - Expected to run in < 6 hour
    - Can only run one job at a time
"""
from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator, ShortCircuitOperator

import dags.hpc.constants as constants
import dags.hpc.utils as hpc_utils
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import LogFileBatchToVerticaLoadCallable, open_external_gate_for_log_type
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

###########################################
# General Variables
###########################################
cadence_in_hours = 12
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4
schedule = None  # Does not run automatically - only run when triggered (i.e. by the cold storage scan dag)

dag_name = "targeting-data-user"

# Prod variables
el_dorado_jar_url = constants.HPC_AWS_EL_DORADO_JAR_URL
meta_data_partition_date_string = "{{ dag_run.conf['metaDataPartitionDate'] }}"
job_environment = TtdEnvFactory.get_from_system()
verticaload_enabled = True if job_environment == TtdEnvFactory.prod else False
gating_type_id = 2000044

# Test variables
# el_dorado_jar_url = "s3://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-3388-use-identitysource-in-cold-storage-scan/latest/el-dorado-assembly.jar"
# meta_data_partition_date_string = "2023-06-26T00:00:00"
# job_environment = TtdEnvFactory.prod
# verticaload_enabled = True
# gating_type_id = 2000042

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 8, 28),
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/L4ACG',
    tags=[hpc.jira_team],
    depends_on_past=True
)

adag = dag.airflow_dag

###########################################
# Targeting data user
###########################################
targeting_data_user_name = 'targeting_data_user_cluster'

standard_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6g.r6g_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

standard_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_8xlarge().with_fleet_weighted_capacity(8),
        R6g.r6g_12xlarge().with_fleet_weighted_capacity(12),
        R6g.r6g_16xlarge().with_fleet_weighted_capacity(16),
    ],
    on_demand_weighted_capacity=45 * 32
)

targeting_data_user_cluster = EmrClusterTask(
    name=targeting_data_user_name,
    master_fleet_instance_type_configs=standard_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=standard_core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

targeting_data_user_props = [
    ('conf', 'spark.network.timeout=3600s'),
    ('conf', 'spark.yarn.max.executor.failures=1000'),
    ('conf', 'spark.yarn.executor.failuresValidityInterval=1h'),
    ('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC'),
]

targeting_data_user_diff_args = [
    ('datetime', meta_data_partition_date_string),
    ('cadenceInHours', cadence_in_hours + constants.TARGETING_DATA_USER_EXTRA_DURATION_IN_HOURS),
]

targeting_data_user_diff_step = EmrJobTask(
    name='Targeting_Data_User_Diff_Step',
    class_name='com.thetradedesk.jobs.receivedcounts.targetingdatauser.TargetingDataUserDiff',
    executable_path=el_dorado_jar_url,
    eldorado_config_option_pairs_list=targeting_data_user_diff_args,
    additional_args_option_pairs_list=targeting_data_user_props,
    timeout_timedelta=timedelta(hours=12),
    cluster_specs=targeting_data_user_cluster.cluster_specs,
    configure_cluster_automatically=True,
    command_line_arguments=['--version']
)

targeting_data_user_cluster.add_parallel_body_task(targeting_data_user_diff_step)

# Vertica Upload
# 1. Choose whether to run VerticaLoad
# 2. Upload _SUCCESS file
# 3. Set up log file within LogWorkflow
# 4. Open gate with LogWorkflow to mark the log file as ready to be collected

# targeting data user diff dataset location
targeting_data_user_diff_dataset_prefix = f"counts/{job_environment.dataset_write_env}/targetingdatauserdiff/v=2/"
targeting_data_user_diff_date = "{{ macros.datetime.fromisoformat(dag_run.conf['metaDataPartitionDate']).strftime('%Y%m%d') }}"
targeting_data_user_diff_hour = "{{ macros.datetime.fromisoformat(dag_run.conf['metaDataPartitionDate']).strftime('%H') }}"
targeting_data_user_diff_partition = f'date={targeting_data_user_diff_date}/hour={targeting_data_user_diff_hour}'
targeting_data_user_diff_date_hour = '{{ dag_run.conf[\'metaDataPartitionDate\'].replace("T", " ") }}'

# 1. Choose whether to run VerticaLoad
is_vertica_load_enabled_task = OpTask(
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


# 2. Upload _SUCCESS file
def add_success_file(**kwargs):
    date_partition = kwargs['date_partition']
    aws_cloud_storage = AwsCloudStorage(conn_id='aws_default')

    key = f"{kwargs['prefix']}{date_partition}/_SUCCESS"
    aws_cloud_storage.load_string(string_data='', key=key, bucket_name=constants.DMP_ROOT, replace=True)

    return f'Written key {key}'


add_targetingdatauserdiff_success_file_task = OpTask(
    op=PythonOperator(
        task_id='add_targetingdatauserdiff_success_file',
        provide_context=True,
        python_callable=add_success_file,
        op_kwargs={
            'prefix': targeting_data_user_diff_dataset_prefix,
            'date_partition': targeting_data_user_diff_partition
        },
        dag=adag
    )
)


# 3. Set up log file within LogWorkflow
def get_vertica_load_object_list(**kwargs):
    log_type_id = kwargs['log_type_id']
    s3_location = kwargs['s3_location']
    log_start_time = kwargs['log_start_time']
    now_utc = datetime.now(timezone.utc)
    logfiletask_endtime = now_utc.strftime("%Y-%m-%dT%H:%M:%S")

    return [(log_type_id, f'{s3_location}', log_start_time, 1, 0, 1, 1, 0, logfiletask_endtime)
            ]  # CloudServiceId 1 == AWS, DataDomain 1 == TTD_RestOfWorld


vertica_load_targetingdatauserdiff_createlogworkflowentry_task = OpTask(
    op=PythonOperator(
        dag=adag,
        python_callable=LogFileBatchToVerticaLoadCallable,
        provide_context=True,
        op_kwargs={
            'database': constants.LOGWORKFLOW_DB,
            'mssql_conn_id': constants.LOGWORKFLOW_CONNECTION if job_environment ==
            TtdEnvFactory.prod else constants.LOGWORKFLOW_SANDBOX_CONNECTION,
            'get_object_list': get_vertica_load_object_list,
            's3_location': f"{targeting_data_user_diff_dataset_prefix}{targeting_data_user_diff_partition}",
            'log_start_time': targeting_data_user_diff_date_hour,
            'log_type_id': 174,  # dbo.fn_Enum_LogType_TargetingDataUser
            'vertica_load_sproc_arguments': {
                'verticaTableId': 105,  # Corresponds to enum dbo.fn_Enum_VerticaTable_ttd_TargetingDataUser()
                'lineCount': 1,
                'verticaTableCopyVersionId': 58105101,
                # to calculate this, call "ThreadSafeMD5.ToHash( <copyColumnListSQL> )" defined in adplatform C# code
                'copyColumnListSQL': 'Tdid;TargetingDataId;MostRecentTimestampUTC;ExpirationTimestampUTC;IdType'
            },
        },
        task_id="vertica_load_targetingdatauserdiff_createlogworkflowentry",
    )
)

# 4. Open External Gate
vertica_load_targetingdatauserdiff_readylogworkflowentry_task = OpTask(
    op=PythonOperator(
        dag=adag,
        python_callable=open_external_gate_for_log_type,
        op_kwargs={
            'mssql_conn_id': constants.LOGWORKFLOW_CONNECTION if job_environment ==
            TtdEnvFactory.prod else constants.LOGWORKFLOW_SANDBOX_CONNECTION,
            'sproc_arguments': {
                'gatingTypeId': gating_type_id,
                'logTypeId': 174,
                'logStartTimeToReady': targeting_data_user_diff_date_hour
            }
        },
        task_id="vertica_load_targetingdatauserdiff_readylogworkflowentry",
    )
)

final_dag_check_task = OpTask(op=FinalDagStatusCheckOperator(dag=adag))
dag >> targeting_data_user_cluster >> is_vertica_load_enabled_task
is_vertica_load_enabled_task >> add_targetingdatauserdiff_success_file_task >> vertica_load_targetingdatauserdiff_createlogworkflowentry_task >> vertica_load_targetingdatauserdiff_readylogworkflowentry_task >> final_dag_check_task
