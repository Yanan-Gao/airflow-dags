import logging
from datetime import datetime, timedelta

from airflow.operators.python import ShortCircuitOperator

from dags.omniux.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

spark_options = [
    ("conf", "spark.driver.cores=28"),
    ("conf", "spark.driver.memory=96G"),
    ("conf", "spark.driver.maxResultSize=0"),
    ("conf", "spark.network.timeout=1440s"),  # default: 120s
    ("conf", "spark.executor.heartbeatInterval=60s"),  # default: 10s
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("conf", "spark.eventLog.rolling.enabled=false"),
    ("conf", "spark.sql.files.maxPartitionBytes=1073741824"),  # 1GB
    ("conf", "spark.scheduler.listenerbus.eventqueue.capacity=30000"),  # default 10000
    ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
]

env_dir = "prod"
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_tags = OMNIUX.omniux().sub_team
    enable_slack_alert = True
else:
    slack_tags = None
    enable_slack_alert = False
    env_dir = "test"

ttd_ctv_bucket = "ttd-ctv"
root_path = "omni-channel-outcomes"
jobQueueRootFolderPath = "job_queue/v=1"
queue_prefix = f"{root_path}/{env_dir}/{jobQueueRootFolderPath}"

batch_size_str = "{{ dag_run.conf.get(\"batch_size\", 50) }}"

run_date_time_str = "{{ (macros.datetime.strptime(dag_run.conf[\"run_date_time\"], \"%Y-%m-%dT%H:%M:%S\") if dag_run.run_type==\"manual\" else data_interval_start).strftime(\"%Y-%m-%dT%H:00:00\") }}"
java_options = [("processJobFromQueue", "true"), ("ttd.ds.ReportParametersDataset.isInChain", "true"),
                ("omniChannelOutcomesPath", f"s3a://{ttd_ctv_bucket}/{root_path}/"), ("runDateTime", run_date_time_str),
                ("jobQueueRootFolderPath", jobQueueRootFolderPath), ("batchSize", batch_size_str), ("pipeline", "BusinessOutcomes"),
                ("ttd.ds.PtcDataset.isInChain", "true")]
job_map = {"report-input": "ReportInput", "impression": "ImpressionPath", "conversion": "ConversionPath", "merge": "MergePath"}
job_retry_delay = timedelta(hours=4)
shared_op_kwargs = {"run_date_time_arg": run_date_time_str}
check_queue_task_id = "check_queue"


def report_input_key(run_date_time: datetime):
    return f"{root_path}/{env_dir}/reportInput/date={run_date_time.strftime('%Y%m%d')}/hour={run_date_time.strftime('%H')}/"


def should_run(queue_size: int, run_date_time: datetime, **kwargs):
    min_queue_size = kwargs["dag_run"].conf.get("min_queue_size", 10)
    always_run_hour = kwargs["dag_run"].conf.get("always_run_hour", 3)
    logging.info(
        f"queue_size={queue_size}, run_date_time={run_date_time}, min_queue_size={min_queue_size}, always_run_hour={always_run_hour}"
    )
    if queue_size <= 0:
        logging.info("Skipping nothing to do.")
        return None
    if run_date_time.hour % 24 == always_run_hour or queue_size > min_queue_size:
        logging.info("Found work to be done.")
        return True
    logging.info("Skipping not enough work to do.")
    return None


def get_queue_size(aws_storage, status):
    queue = f"{queue_prefix}/status={status}"
    logging.info(f"Fetching files from bucket: {ttd_ctv_bucket} and prefix: {queue}")
    # fetch files in queue
    queue_list = aws_storage.list_keys(
        bucket_name=ttd_ctv_bucket,
        prefix=queue,
    )
    queue_list = list(filter(lambda path: path.endswith('.json'), queue_list))
    queue_size = len(queue_list)
    logging.info(f"Queue size for status ({status}) = {queue_size}\n{queue_list}")
    return queue_size


def check_queue(run_date_time_arg: str, **kwargs):
    """
    Checks if there are items to be processed.
    """
    run_date_time = datetime.strptime(run_date_time_arg, "%Y-%m-%dT%H:%M:%S")
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    queue_size = 0

    for status in ['pending', 'processing']:
        queue_size += get_queue_size(aws_storage, status)

    logging.info(f"Total queue size: {queue_size}, run_date_tme={run_date_time}")
    validate_report_input(aws_storage, run_date_time)
    return should_run(queue_size, run_date_time, **kwargs)


def validate_report_input(aws_storage, run_date_time: datetime):
    data_set_path = report_input_key(run_date_time)
    keys = aws_storage.list_keys(prefix=data_set_path, bucket_name=ttd_ctv_bucket)
    if len(keys):
        logging.error(f"Input files found: {keys}\n Clean these files before running.")
        raise Exception("Report input directory is not clean.")


def check_for_report_input_dataset(run_date_time_arg: str, **kwargs):
    run_date_time = datetime.strptime(run_date_time_arg, "%Y-%m-%dT%H:%M:%S")
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    key = f"{report_input_key(run_date_time)}_SUCCESS"
    logging.info(f"Checking for bucket:{ttd_ctv_bucket}, file: {key}")
    if aws_storage.check_for_key(key=key, bucket_name=ttd_ctv_bucket):
        logging.info("Found")
        return True
    logging.error(f"Dataset not found bucket:{ttd_ctv_bucket}, file: {key}")
    return None


# region add steps to dag
eldorado_dag = TtdDag(
    dag_id='ctv-omnichannel-outcomes-on-demand',
    start_date=datetime(2024, 7, 25, 0, 0),
    schedule_interval=timedelta(hours=1),
    retries=0,
    retry_delay=job_retry_delay,
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    tags=[OMNIUX.team.name, "omnichannel", "omnichannel-outcomes"],
    run_only_latest=True
)
dag = eldorado_dag.airflow_dag


def emr_task(name, class_name, on_demand_weighted_capacity, master_instance_type=M5d.m5d_8xlarge()):
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            # 24xlarge Instances
            R5a.r5a_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_24xlarge().cores),
            R5.r5_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_24xlarge().cores),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_24xlarge().cores),

            # 16xlarge Instances
            R5a.r5a_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_16xlarge().cores),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_16xlarge().cores),
            R5.r5_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_16xlarge().cores),

            # 12xlarge Instances
            R5a.r5a_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_12xlarge().cores),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_12xlarge().cores),

            # 8xlarge Instances
            R5a.r5a_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_8xlarge().cores),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_8xlarge().cores),
        ],
        on_demand_weighted_capacity=on_demand_weighted_capacity * R5a.r5a_24xlarge().cores
    )

    # Define Cluster
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[master_instance_type.with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    cluster = EmrClusterTask(
        name=f'process-{name}-path',
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags={"Team": OMNIUX.team.jira_team},
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        retries=0
    )

    # Add step to cluster
    step = EmrJobTask(
        name=f'process-{name}-path_step',
        class_name=class_name,
        executable_path=get_jar_file_path(),
        configure_cluster_automatically=True,
        additional_args_option_pairs_list=spark_options,
        eldorado_config_option_pairs_list=java_options + [("jobs", job_map[name])]
    )
    cluster.add_parallel_body_task(step)
    return cluster


impression_path_cluster = emr_task('impression', 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 35)
conversion_path_cluster = emr_task('conversion', 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 40)
merge_task_cluster = emr_task(
    'merge', 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 1, master_instance_type=M5d.m5d_2xlarge()
)
report_input_task_cluster = emr_task(
    'report-input', 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 1, master_instance_type=M5d.m5d_2xlarge()
)

check_queue_op = OpTask(
    op=ShortCircuitOperator(
        task_id=check_queue_task_id, provide_context=True, python_callable=check_queue, dag=dag, retries=0, op_kwargs=shared_op_kwargs
    )
)
check_input_op = OpTask(
    op=ShortCircuitOperator(
        task_id="check_input",
        provide_context=True,
        python_callable=check_for_report_input_dataset,
        dag=dag,
        retries=0,
        op_kwargs=shared_op_kwargs
    )
)
eldorado_dag >> check_queue_op >> report_input_task_cluster >> check_input_op >> impression_path_cluster >> merge_task_cluster
check_input_op >> conversion_path_cluster >> merge_task_cluster
