"""
Merge LAL Model Results job and publish merged results to Aerospike

Job Details:
    - Runs every 24 hours
    - Terminate in 2 hours if not finished
    - After merging results, the next step is publishing results
"""
from enum import Enum

from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator

from dags.adpb.spark_jobs.shared.adpb_helper import LalAerospikeConstant
from ttd.cloud_provider import CloudProviders
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.constants import DataTypeId
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask

calculation_date = "{{ ds }}"
calculation_date_nodash = "{{ ds_nodash }}"
PERSON_VENDOR_ID_IAV2 = "10"


# These constants should be in sync with Eldorado's LalVersionUtils.LookalikeVersionType
class LookalikeVersionType(Enum):
    deviceIdsOverlaps = 1
    seedXdExpansionDeviceIdsOverlaps = 2
    userEmbedding = 3
    firstPartyDeviceIdsOverlaps = 4


allowed_data_types_for_lal = [
    DataTypeId.IPAddressRange.value, DataTypeId.TrackingTag.value, DataTypeId.ImportedAdvertiserData.value, DataTypeId.CrmData.value
]
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
"""

"""
sibv2_publish_lal = TtdDag(
    dag_id="adpb-sibv2-publish-lal",
    # Current LAL process runs at 7am UTC, and it takes around 5 hours, we start this process 8 hours later
    start_date=datetime(2024, 8, 28, 15, 0),
    schedule_interval=timedelta(hours=24),
    retries=3,
    retry_delay=timedelta(hours=7),
    max_active_runs=1,
    slack_channel="#scrum-adpb-alerts"
)

dag = sibv2_publish_lal.airflow_dag

spark_config_options = [("executor-memory", "35G"), ("conf", "spark.executor.cores=5"), ("conf", "spark.dynamicAllocation.enabled=false"),
                        ("conf", "spark.executor.instances=179"), ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                        ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"), ("conf", "spark.driver.maxResultSize=6G"),
                        ("conf", "spark.network.timeout=1200s"), ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                        ("conf", "spark.sql.shuffle.partitions=6000"), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

##############################################################################
# Config for push to aerospike step
##############################################################################

job_name = "etl-lal-push-data-to-aerospike"
push_step_name = "push-lal-data-to-aerospike"
aerospike_namespace = LalAerospikeConstant.aerospike_namespace
aerospike_set = LalAerospikeConstant.aerospike_set

db_name = "datapipeline"
s3_bucket = "ttd-identity"
model_results_path_prefix = "s3://" + s3_bucket + "/" + db_name + "/prod/models/lal/daily_model_results_for_aerospike/v=1/date="
override_batch_name = "override"
push_first_party_step_name = "push-first-party-data-to-aerospike"
first_party_batch_name = "firstparty"
push_ap_custom_segment_step_name = "push-ap-custom-segment-to-aerospike"
ap_custom_segment_batch_name = "apcustom"

aerospike_address = LalAerospikeConstant.aerospike_address
ttl = LalAerospikeConstant.ttl
max_map_size_for_top_non_overlap = 50000
max_map_size_for_top_overlap = 200000
max_map_size = max_map_size_for_top_non_overlap + max_map_size_for_top_overlap  # how big can our maps be in aerospike
minimum_sibv2_uniques_users = 6100

lal_push_to_aerospike_spark_options = [("executor-memory", "95G"), ("executor-cores", "15"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=95G"), ("conf", "spark.driver.maxResultSize=95G"),
                                       ("conf", "spark.sql.shuffle.partitions=3000"),
                                       ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

job_ec2_subnet_id = LalAerospikeConstant.job_ec2_subnet_id
job_emr_managed_master_security_group = LalAerospikeConstant.job_emr_managed_master_security_group
job_emr_managed_slave_security_group = LalAerospikeConstant.job_emr_managed_slave_security_group
job_service_access_security_group = LalAerospikeConstant.job_service_access_security_group

lal_aerospike_push_cluster = EmrClusterTask(
    name=job_name + "_cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5a.m5a_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        'Team': 'ADPB',
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5a.r5a_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=20,
    ),
    ec2_subnet_ids=[job_ec2_subnet_id],
    emr_managed_master_security_group=job_emr_managed_master_security_group,
    emr_managed_slave_security_group=job_emr_managed_slave_security_group,
    service_access_security_group=job_service_access_security_group,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

#############################################################################################
#  Based on testing, the data push step has taken about 15 minutes but setting the timeout
#  to be 2 hours as a generous threshold.
#############################################################################################

prod_push_to_aerospike_spark_step = EmrJobTask(
    name=push_step_name,
    class_name="jobs.lal.PublishToAerospikeLal",
    executable_path=jar_path,
    additional_args_option_pairs_list=lal_push_to_aerospike_spark_options,
    eldorado_config_option_pairs_list=[
        ("modelResultsPath", model_results_path_prefix + calculation_date_nodash + "/batch=" + override_batch_name),
        ("aerospikeAddress", aerospike_address), ("aerospikeNamespace", aerospike_namespace), ("ttl", ttl), ("aerospikeSet", aerospike_set),
        ("maxMapSize", max_map_size), ("isTestRun", "false")
    ],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=lal_aerospike_push_cluster.cluster_specs,
)

prod_push_ap_custom_segment_score_to_aerospike_spark_step = EmrJobTask(
    name=push_ap_custom_segment_step_name,
    class_name="jobs.lal.PublishToAerospikeLal",
    executable_path=jar_path,
    additional_args_option_pairs_list=lal_push_to_aerospike_spark_options,
    eldorado_config_option_pairs_list=[
        ("modelResultsPath", model_results_path_prefix + calculation_date_nodash + "/batch=" + ap_custom_segment_batch_name),
        ("aerospikeAddress", aerospike_address), ("aerospikeNamespace", aerospike_namespace), ("ttl", ttl), ("aerospikeSet", aerospike_set),
        ("maxMapSize", max_map_size), ("isTestRun", "false"), ("metricsPrefix", "ap_custom_segment_lal_")
    ],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=lal_aerospike_push_cluster.cluster_specs,
)

##############################################################################
# Config for DailyRefreshLalModelsInAerospike
##############################################################################

user_lal_generate_daily_refresh_aerospike_cluster = EmrClusterTask(
    name="GenerateDailyRefreshLalModelsInAerospike",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": slack_groups.ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=960
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

lal_generate_daily_refresh_aerospike_step_options = [
    ("date", calculation_date),
    ("maxMapSize", max_map_size_for_top_overlap),
    ("inputLalPath", "models/lal/all_model_results/v=3/xdvendorid=10"),
    ("defaultLookalikeVersionType", LookalikeVersionType.seedXdExpansionDeviceIdsOverlaps.value),
    ("rollOutRelevanceScoreMode", "false"),
    ("shouldWriteUEScores", "false"),
    ("overrideBatchName", override_batch_name),
    ("isTestMode", "false"),
    ("shouldWriteAPCustomSegmentModellingSourceScores", "true"),
    ("minUniquesForTopNonOverlapScoreSegments", minimum_sibv2_uniques_users),
]

lal_generate_daily_refresh_aerospike_step = EmrJobTask(
    name="LAL-Generate-Daily-Refresh-LalModels-InAerospike",
    class_name="jobs.lal.DailyRefreshLalAerospike",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_config_options,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=lal_generate_daily_refresh_aerospike_step_options,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=user_lal_generate_daily_refresh_aerospike_cluster.cluster_specs,
)

check_1p_xd_expansion_lal_results_data = OpTask(
    op=DatasetRecencyOperator(
        dag=sibv2_publish_lal.airflow_dag,
        datasets_input=[Datasources.lal.model_results(True, PERSON_VENDOR_ID_IAV2, version=3)],
        cloud_provider=CloudProviders.aws,
        run_delta=timedelta(days=0),  # check yesterday log is available
        task_id='check_1p_xd_lal_results_data'
    )
)

job_datetime_format: str = "%Y-%m-%dT%H:00:00"
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
logworkflow_connection = 'lwdb'
gating_type_id = 2000364


def _get_time_slot(dt: datetime):
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


def _open_lwdb_gate(**context):
    dt = _get_time_slot(context['data_interval_start'])
    log_start_time = dt.strftime('%Y-%m-%d %H:00:00')
    ExternalGateOpen(
        mssql_conn_id=logworkflow_connection,
        sproc_arguments={
            'gatingType': gating_type_id,
            'grain': TaskBatchGrain_Daily,
            'dateTimeToOpen': log_start_time
        }
    )


lal_model_count_sql_import_open_gate = OpTask(
    op=PythonOperator(
        task_id='open_lwdb_gate',
        python_callable=_open_lwdb_gate,
        provide_context=True,
        dag=sibv2_publish_lal.airflow_dag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=sibv2_publish_lal.airflow_dag))

user_lal_generate_daily_refresh_aerospike_cluster.add_sequential_body_task(lal_generate_daily_refresh_aerospike_step)

lal_aerospike_push_cluster.add_sequential_body_task(prod_push_to_aerospike_spark_step)
lal_aerospike_push_cluster.add_sequential_body_task(prod_push_ap_custom_segment_score_to_aerospike_spark_step)

sibv2_publish_lal >> check_1p_xd_expansion_lal_results_data >> user_lal_generate_daily_refresh_aerospike_cluster >> lal_aerospike_push_cluster
lal_aerospike_push_cluster >> lal_model_count_sql_import_open_gate >> final_dag_status_step
