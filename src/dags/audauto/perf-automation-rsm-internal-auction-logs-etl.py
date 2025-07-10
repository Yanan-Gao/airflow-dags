import copy
from datetime import timedelta, datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask

# generic spark settings list we'll add to each step.
spark_options_list = [("executor-memory", "31G"), ("executor-cores", "5"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=5G"),
                      ("conf", "spark.driver.maxResultSize=5G"), ("conf", "spark.sql.shuffle.partitions=1440"),
                      ("conf", "spark.default.parallelism=1440")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3_2

DATE_TIME = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"

INTERNAL_AUCTION_JAR = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar"

# Define Dag
internal_auction_etl = TtdDag(
    dag_id="perf-automation-rsm-internal-auction-logs-etl",
    start_date=datetime(year=2024, month=12, day=15, hour=2),
    schedule_interval=timedelta(hours=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/yrMMCQ',
    retries=0,
    max_active_runs=4,
    retry_delay=timedelta(hours=1),
    tags=['DATPERF'],
    enable_slack_alert=False,
    run_only_latest=False
)

ia_dag = internal_auction_etl.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
rsm_ia_result_log_dataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="rsminternalauction",
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None,
    env_aware=False,
).with_check_type(check_type="hour")

###############################################################################
# S3 dataset sensors
###############################################################################
rsm_ia_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='rsm_ia_log_data_available',
        datasets=[rsm_ia_result_log_dataset],
        ds_date='{{(data_interval_start + macros.timedelta(hours=1)).to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 3,
    )
)

###############################################################################
# clusters
###############################################################################
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_4xlarge().with_fleet_weighted_capacity(16).with_ebs_size_gb(512),
    ],
    on_demand_weighted_capacity=768,
)

rsm_ia_etl_cluster = EmrClusterTask(
    name="RSMIAEtlCluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={'Team': DATPERF.team.jira_team},
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

rsm_ia_etl_step = EmrJobTask(
    name="ConvertRSMInternalAuctionLogsParquet",
    class_name="com.thetradedesk.jobs.internalauctionlog.InternalAuctionLogETL",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=[('date', DATE_TIME), ('partitions', 1440), ('fileCount', 1440),
                                       ('logType', 'RSMInternalAuctionLog')],
    executable_path=INTERNAL_AUCTION_JAR,
    timeout_timedelta=timedelta(hours=6)
)

# add step to cluster
rsm_ia_etl_cluster.add_parallel_body_task(rsm_ia_etl_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=ia_dag))

# Flow

internal_auction_etl >> rsm_ia_dataset_sensor >> rsm_ia_etl_cluster >> final_dag_status_step
