import copy
import logging
from datetime import date, datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from datasources.sources.sib_datasources import SibDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.cluster_params import calc_cluster_params
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import ADPB
from ttd.ttdenv import TtdEnvFactory

from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

pipeline_name = "adpb-sibv2-group-data-pipeline-rolling"
owner = ADPB.team
slack_tags = owner.sub_team

# Job config
job_start_date = datetime(2024, 8, 13, 6, 0)
job_schedule_interval = timedelta(hours=12)
job_environment = TtdEnvFactory.get_from_system()
env_str = "prod" if job_environment == TtdEnvFactory.prod else "test"
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
# Execution date time macros
date_time = '{{ data_interval_end.strftime(\"%Y-%m-%dT%H:00:00\") }}'

run_date_str = '{{ data_interval_end.strftime(\"%Y%m%d\") }}'
run_date_iso_str = '{{ data_interval_end.strftime(\"%Y-%m-%d\") }}'
run_hour_str = '{{ data_interval_end.strftime(\"%H\") }}'

# Sib check config
sib_lookback_days = 5
sib_date_key = "sib_date_key"
check_sib_date_task_id = "get-most-recent-sib-date"
sib_date_value = "{{ task_instance.xcom_pull(dag_id='" + pipeline_name + "', task_ids='" + check_sib_date_task_id + "', key='" + sib_date_key + "') }}"

# Cluster config
cluster_idle_timeout_seconds = 30 * 60  # 30 mins
num_workers = 240  # r5_4xlarge
ebs_size = 256  # gb
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5

application_configuration = [
    {
        'Classification': 'spark',
        'Properties': {
            'maximizeResourceAllocation': 'true'
        }
    },
    {
        "Classification": "emrfs-site",
        "Properties": {
            "fs.s3.maxConnections": "500",
            "fs.s3.maxRetries": "50",
            "fs.s3.sleepTimeSeconds": "10"
        }
    },
]


def generate_spark_option_list(instances_count, instance_type):
    cluster_params = calc_cluster_params(
        instances=instances_count,
        vcores=instance_type.cores,
        memory=instance_type.memory,
        parallelism_factor=2,
        max_cores_executor=instance_type.cores
    )
    spark_options_list = [("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.dynamicAllocation.enabled=false"),
                          ("conf", f"spark.default.parallelism={cluster_params.parallelism}"),
                          ("conf", f"spark.sql.shuffle.partitions={cluster_params.parallelism}"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
                          ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                          ('conf', 'spark.kryoserializer.buffer.max=512m'), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]
    return spark_options_list


cluster_tags = {
    'Team': ADPB.team.jira_team,
}

sibv2_group_data_rolling_dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    depends_on_past=False,
    slack_tags=slack_tags,
    tags=[owner.jira_team],
    retries=0
)


# get recent sibv2 main log date
def get_sib_date(**kwargs):
    run_date_iso_str_arg = kwargs['run_date_iso_str']
    current_run_date = datetime.strptime(run_date_iso_str_arg, '%Y-%m-%d').date()

    hook = AwsCloudStorage(conn_id='aws_default')
    check_recent_uniques_date = SibDatasources.sibv2_device_data_uniques.check_recent_data_exist(hook, date.today(), sib_lookback_days)
    if not check_recent_uniques_date:
        raise ValueError(f'Could not find sibv2 in last {sib_lookback_days} days')

    check_recent_bitmap_date = SibDatasources.sibv2_device_seven_day_rollup_index_bitmap.check_recent_data_exist(
        hook, date.today(), sib_lookback_days
    )
    if not check_recent_bitmap_date:
        raise ValueError(f'Could not find sibv2 bitmap in last {sib_lookback_days} days')

    # get the most recent date
    latest_sib_date = min(check_recent_uniques_date.get(), check_recent_bitmap_date.get())
    if current_run_date < latest_sib_date:
        logging.warning(f'Current run date: {current_run_date} is behind latest sibv2 date: {latest_sib_date}')
    sib_date = min(current_run_date, latest_sib_date)

    kwargs['task_instance'].xcom_push(key=sib_date_key, value=sib_date)
    test_date = kwargs['task_instance'].xcom_pull(dag_id=pipeline_name, task_ids=check_sib_date_task_id, key=sib_date_key)
    logging.info(f'Found sibv2 date: {test_date}')


check_recent_sib_date_op_task = OpTask(
    op=PythonOperator(
        task_id=check_sib_date_task_id,
        python_callable=get_sib_date,
        dag=sibv2_group_data_rolling_dag.airflow_dag,
        op_kwargs={'run_date_iso_str': run_date_iso_str},
        provide_context=True
    )
)


# write sib date to s3: _SIB_DATE
def write_sib_date_file(**kwargs):
    run_date = kwargs['task_instance'].xcom_pull(dag_id=pipeline_name, task_ids=check_sib_date_task_id, key=sib_date_key)
    hook = AwsCloudStorage(conn_id='aws_default')
    path = kwargs['path']
    success_s3_key = f'{path}/_SUCCESS'
    sib_date_s3_key = f'{path}/_SIB_DATE'
    bucket = kwargs['bucket']
    if not hook.check_for_key(key=success_s3_key, bucket_name=bucket):
        logging.warning(f"Success file not found: {success_s3_key}")
        raise AirflowSkipException
    logging.info(f"Write sib date '{run_date}' to file {sib_date_s3_key}")
    hook.load_string(string_data=run_date.strftime("%Y-%m-%d"), key=sib_date_s3_key, bucket_name=bucket, replace=True)
    return f'{sib_date_s3_key} - {run_date}'


write_sib_date_file_op_task = OpTask(
    op=PythonOperator(
        task_id='add_sib_file',
        provide_context=True,
        op_kwargs={
            'path': f"datapipeline/{env_str}/seeninbiddingrollingdevicedatauniques/v=2/date={run_date_str}/hour={run_hour_str}",
            'bucket': "ttd-identity"
        },
        python_callable=write_sib_date_file,
        dag=sibv2_group_data_rolling_dag.airflow_dag,
    )
)

write_bitmap_sib_date_file_op_task = OpTask(
    op=PythonOperator(
        task_id='add_bitmap_sib_file',
        provide_context=True,
        op_kwargs={
            'path':
            f"datapipeline/{env_str}/seeninbiddingrollingdevicesevendayrollupindexbitmap/v=2/date={run_date_str}/hour={run_hour_str}",
            'bucket': "ttd-identity"
        },
        python_callable=write_sib_date_file,
        dag=sibv2_group_data_rolling_dag.airflow_dag,
    )
)

rolling_device_expansion_uniques_overlaps_cluster = EmrClusterTask(
    name="adpb-sibv2-rolling-device-data-rollup-and-uniques",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(ebs_size).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_8xlarge().with_ebs_size_gb(ebs_size * 2).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(ebs_size * 3).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
        ],
        on_demand_weighted_capacity=num_workers,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment
)

# EMR step 1. generate seed expansion
eldorado_options_seed_expansion = [
    ("dateTime", date_time),
    ("sibDate", sib_date_value),
    ("generatePrometheusMetrics", 'true'),
]

spark_options_expansion_uniques_overlaps = generate_spark_option_list(num_workers, R7g.r7g_4xlarge())

rolling_device_seed_expansion_step = EmrJobTask(
    name="rolling-device-seed-expansion",
    class_name="jobs.agg_etl.SeenInBiddingV2RollingSeedExpansionJob",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_expansion_uniques_overlaps,
    eldorado_config_option_pairs_list=eldorado_options_seed_expansion,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rolling_device_expansion_uniques_overlaps_cluster.cluster_specs,
)

# EMR step 2. generate bitmaps
eldorado_options_bitmaps = [("dateTime", date_time), ("sibDate", sib_date_value)]

rolling_device_data_bitmaps_step = EmrJobTask(
    name="rolling-device-bitmaps",
    class_name="jobs.agg_etl.SeenInBiddingV2RollingDeviceBitmapGenerator",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_expansion_uniques_overlaps,
    eldorado_config_option_pairs_list=eldorado_options_bitmaps,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rolling_device_expansion_uniques_overlaps_cluster.cluster_specs,
)

# EMR step 3. generate uniques and overlaps
eldorado_options_uniques_and_overlaps = [("dateTime", date_time), ("sibDate", sib_date_value),
                                         ("isOfflineTrackingTagExpansionIncluded", "true")]

rolling_device_data_uniques_and_overlaps_step = EmrJobTask(
    name="rolling-device-uniques-and-overlaps",
    class_name="jobs.agg_etl.SeenInBiddingV2RollingDeviceUniquesAndOverlapsJob",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_expansion_uniques_overlaps,
    eldorado_config_option_pairs_list=eldorado_options_uniques_and_overlaps,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rolling_device_expansion_uniques_overlaps_cluster.cluster_specs,
)

# Add steps to clusters
rolling_device_expansion_uniques_overlaps_cluster.add_sequential_body_task(rolling_device_seed_expansion_step)
rolling_device_expansion_uniques_overlaps_cluster.add_sequential_body_task(rolling_device_data_bitmaps_step)
rolling_device_expansion_uniques_overlaps_cluster.add_sequential_body_task(rolling_device_data_uniques_and_overlaps_step)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=sibv2_group_data_rolling_dag.airflow_dag, trigger_rule="none_failed"))

# Assemble
rolling_device_seed_expansion_step >> rolling_device_data_uniques_and_overlaps_step
sibv2_group_data_rolling_dag >> check_recent_sib_date_op_task >> rolling_device_expansion_uniques_overlaps_cluster >> write_sib_date_file_op_task >> final_dag_check
rolling_device_expansion_uniques_overlaps_cluster >> write_bitmap_sib_date_file_op_task >> final_dag_check

airflow_dag = sibv2_group_data_rolling_dag.airflow_dag
