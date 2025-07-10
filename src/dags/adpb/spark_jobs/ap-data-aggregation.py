from typing import List, Tuple

from airflow.exceptions import AirflowNotFoundException
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

from airflow.operators.python import PythonOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.ec2.emr_instance_types.compute_optimized.c7gd import C7gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

from datetime import datetime, timedelta

import logging

from ttd.slack.slack_groups import ADPB

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

cluster_name = "ApDataAggregation"

datapipe_bucket = "ttd-datapipe-data"
bidfeedback_prefix = "parquet/rtb_bidfeedback_cleanfile/v=5"
conversion_prefix = "parquet/rtb_conversiontracker_cleanfile/v=5"
event_prefix = "parquet/rtb_eventtracker_verticaload/v=4"

job_step_retries: int = 2
job_step_retry_delay: timedelta = timedelta(minutes=30)

# raw data v5 has a pretty big lag, jobs would still pick it up as soon as it's available
job_data_complete_step_retries: int = 12
job_data_complete_step_retry_delay = timedelta(minutes=20)

java_settings_list: List[Tuple[str, str]] = []

spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=5G"),
                      ("conf", "spark.sql.shuffle.partitions=3000"), ("conf", "spark.driver.maxResultSize=5G"),
                      ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]


def check_source_data_complete(job_date_time_str: str, **kwargs):
    job_date_time = datetime.strptime(job_date_time_str, "%Y-%m-%dT%H:00:00")
    aws_storage = AwsCloudStorage(conn_id='aws_default')

    date_str = job_date_time.strftime("%Y%m%d")
    hour_str = f"{job_date_time.hour:0>2d}"
    success_file_name = f"_SUCCESS-sx-{date_str}-{hour_str}"

    bidfeedback_success_file = f"{bidfeedback_prefix}/{success_file_name}"
    conversion_success_file = f"{conversion_prefix}/{success_file_name}"
    event_success_file = f"{event_prefix}/{success_file_name}"

    logging.info(f"Checking if input data exists for {job_date_time_str}")

    logging.info(f'Checking BidFeedback Data for {job_date_time_str}. Bucket: {datapipe_bucket} File: {bidfeedback_success_file}')
    if not aws_storage.check_for_key(bucket_name=datapipe_bucket, key=bidfeedback_success_file):
        logging.info(f' Bucket: {datapipe_bucket} File: {bidfeedback_success_file} Partition: {job_date_time_str} is not complete')
        raise AirflowNotFoundException

    logging.info(f'Checking Conversion Data for {job_date_time_str}. Bucket: {datapipe_bucket} File: {conversion_success_file}')
    if not aws_storage.check_for_key(bucket_name=datapipe_bucket, key=conversion_success_file):
        logging.info(f' Bucket: {datapipe_bucket} File: {conversion_success_file} Partition: {job_date_time_str} is not complete')
        raise AirflowNotFoundException

    logging.info(f'Checking Event Data for {job_date_time_str}. Bucket: {datapipe_bucket} File: {event_success_file}')
    if not aws_storage.check_for_key(bucket_name=datapipe_bucket, key=event_success_file):
        logging.info(f' Bucket: {datapipe_bucket} File: {event_success_file} Partition: {job_date_time_str} is not complete')
        raise AirflowNotFoundException
    return True


ap_data_aggregation = TtdDag(
    dag_id="adpb-ap-data-aggregation",
    # job_start_date = datetime(2020, 10, 30, 0) - timedelta(hours=1),
    start_date=datetime(2024, 7, 25, 18) - timedelta(hours=1),
    schedule_interval=timedelta(hours=1),
    max_active_runs=3,
    slack_channel="#scrum-adpb-alerts",
    retries=job_step_retries,
    retry_delay=job_step_retry_delay,
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=False,
)

dag = ap_data_aggregation.airflow_dag

cluster = EmrClusterTask(
    name="ApDataAggregation",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[C7g.c7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            C7g.c7g_2xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            C7g.c7g_4xlarge().with_ebs_size_gb(128).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            C7g.c7g_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(8),
            C7gd.c7gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            C7gd.c7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4),
            C7gd.c7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(8),
        ],
        on_demand_weighted_capacity=8
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

check_source_data_source_step = OpTask(
    op=PythonOperator(
        task_id='check_incoming_data_exists',
        dag=ap_data_aggregation.airflow_dag,
        python_callable=check_source_data_complete,
        op_kwargs=dict(job_date_time_str='{{ data_interval_start.strftime("%Y-%m-%dT%H:00:00") }}'),
        # Makes it wait for source data for one day (given timeout between retries of 2 hours)
        retries=job_data_complete_step_retries,
        retry_delay=job_data_complete_step_retry_delay,
        provide_context=True
    )
)

bidfeedback_agg_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="BidFeedbackAgg",
    class_name="jobs.agg_etl.BidFeedbackUserSiteLogAgg",
    timeout_timedelta=timedelta(minutes=45),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("runtime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}")],
    executable_path=jar_path
)

conversion_tracker_agg_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="ConversionTrackerAgg",
    class_name="jobs.agg_etl.ConversionTrackerAgg",
    timeout_timedelta=timedelta(minutes=30),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("runtime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}")],
    executable_path=jar_path
)

event_tracker_agg_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="EventTrackerAgg",
    class_name="jobs.agg_etl.EventTrackerAgg",
    timeout_timedelta=timedelta(minutes=30),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("runtime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}")],
    executable_path=jar_path
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=ap_data_aggregation.airflow_dag))
cluster.add_sequential_body_task(bidfeedback_agg_step)
cluster.add_sequential_body_task(conversion_tracker_agg_step)
cluster.add_sequential_body_task(event_tracker_agg_step)

ap_data_aggregation >> check_source_data_source_step >> cluster >> check
