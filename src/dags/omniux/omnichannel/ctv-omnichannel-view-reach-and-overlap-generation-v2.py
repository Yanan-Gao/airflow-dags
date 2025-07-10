import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from dags.omniux.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExecuteOnDemandDataMove
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory  # TestEnv

job_name = "ctv-omnichannel-view-reach-and-overlap-generation-v2"
start_date = datetime(2024, 6, 26, 0, 0)

env = TtdEnvFactory.get_from_system()

# Use TestEnv when testing and using
# ('ttd.OmnichannelSeedBatchDataset.isInChain', 'true'),
# ('ttd.OmnichannelReachAndOverlapSnapshotDataSet.isInChain', 'true'),
# ('ttd.OmnichannelReachAndOverlapDeltaDataset.isInChain', 'true'),

# env = TestEnv()

# LWDB configs
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"
logworkflow_connection_open_gate = logworkflow_connection if env == TtdEnvFactory.prod \
    else logworkflow_sandbox_connection

ttdDag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    enable_slack_alert=True,
    tags=[OMNIUX.team.name, "OmnichannelView"],
    run_only_latest=True
)

dag = ttdDag.airflow_dag


def has_seeds_to_process(**kwargs):
    ds = kwargs['ds']
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    bucket_name = "ttd-ctv"
    key_prefix = f"{env.dataset_read_env}/OmnichannelView/OmnichannelSeedBatch/v=1/date={ds.replace('-', '')}/hour=0/_COUNT"
    logging.info(f"Reading key {key_prefix}")
    count_str = aws_storage.read_key(bucket_name=bucket_name, key=key_prefix).strip()
    logging.info(f"Read value {count_str}")
    count = int(count_str)
    logging.info(f"Batch count {count}")
    if count > 0:
        return True
    logging.info("No work")


cluster_task_id_base = "reach-and-overlap-cluster"

capacity = 10

reach_and_overlap_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

reach_and_overlap_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(M5.m5_8xlarge().cores),
        M5.m5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(M5.m5_16xlarge().cores),
        R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_8xlarge().cores),
        R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_12xlarge().cores)
    ],
    on_demand_weighted_capacity=capacity * M5.m5_24xlarge().cores
)

reach_and_overlap_cluster_task = EmrClusterTask(
    name=f"{cluster_task_id_base}",
    master_fleet_instance_type_configs=reach_and_overlap_master_fleet_instance_type_configs,
    cluster_tags={"Team": OMNIUX.team.jira_team},
    core_fleet_instance_type_configs=reach_and_overlap_core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    retries=0
)

reach_and_overlap_job_task = EmrJobTask(
    name="reach-and-overlap",
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.omnichannelview.OmnichannelReachAndOverlapGenerationV2",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=[
        ("conf", "spark.kryoserializer.buffer.max=512m"),
        ("conf", "spark.driver.maxResultSize=2048m"),
    ],
    eldorado_config_option_pairs_list=[
        ("runStartInclusive", "{{ data_interval_start.strftime(\"%Y-%m-%dT00:00:00\") }}"),
        ("runEndExclusive", "{{ data_interval_end.strftime(\"%Y-%m-%dT00:00:00\") }}"),
        ('relevanceThreshold', '2.0'),
        ('householdCrossJoinPartitions', '2000'),
        ('snapshotOutputPartitions', '10'),
        ('maxAgeInDays', '56'),  # two months
    ],
    retries=0
)

seed_batch_job_task = EmrJobTask(
    name="seed-batch-job-task",
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.omnichannelview.OmnichannelSeedBatchGeneration",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ("runStartInclusive", "{{ data_interval_start.strftime(\"%Y-%m-%dT00:00:00\") }}"),
        ("runEndExclusive", "{{ data_interval_end.strftime(\"%Y-%m-%dT00:00:00\") }}"),
        ('maxBatchSize', '100000'),
    ],
    retries=0
)

has_seeds_to_process_op = OpTask(
    op=ShortCircuitOperator(task_id="has-seed-to-process", provide_context=True, python_callable=has_seeds_to_process, dag=dag, retries=0)
)

logworkflow_open_omnichannel_sql_import_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExecuteOnDemandDataMove,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'taskId': 1000437,  # dbo.fn_Enum_Task_OnDemandImportOmnichannelReachAndOverlap()
                'prefix': 'date={{ data_interval_start.strftime("%Y%m%d") }}/'
            }
        },
        task_id="logworkflow_trigger_omnichannel_sql_import_on_demand",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
)

reach_and_overlap_cluster_task.add_sequential_body_task(seed_batch_job_task)
reach_and_overlap_cluster_task.add_sequential_body_task(has_seeds_to_process_op)
reach_and_overlap_cluster_task.add_sequential_body_task(reach_and_overlap_job_task)
reach_and_overlap_cluster_task.add_sequential_body_task(logworkflow_open_omnichannel_sql_import_gate)

ttdDag >> reach_and_overlap_cluster_task
