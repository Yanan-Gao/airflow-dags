import copy
from datetime import timedelta, datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [("executor-memory", "204G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=110G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=4096"),
                      ("conf", "spark.default.parallelism=4096"), ("conf", "spark.driver.maxResultSize=50G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

model = "RSM"
run_date = "{{ data_interval_start.to_date_string() }}"
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"

short_date_str = "{{ data_interval_start.strftime(\"%Y%m%d\") }}"
ial_path = f"s3://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date={short_date_str}"

rsm_etl_dag = TtdDag(
    dag_id="perf-automation-rsm-etl",
    start_date=datetime(2024, 12, 2, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO", "RSM"]
)

adag = rsm_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
policytable_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata",
    data_name="audience/policyTable/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=True,
)

aggregatedseed_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="audience/aggregatedSeed",
    version=1,
    env_aware=True,
)

bidsimpressions_data = HourGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/koav4/v=1/prod",
    data_name="bidsimpressions",
    date_format="year=%Y/month=%m/day=%d",
    hour_format="hourPart={hour}",
    env_aware=False,
    version=None,
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[policytable_dataset, aggregatedseed_dataset, bidsimpressions_data],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 23,
    )
)

###############################################################################
# clusters
###############################################################################
rsm_etl_cluster_task = EmrClusterTask(
    name="RSM_ETL_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_ebs_iops(16000).with_ebs_throughput(750).with_max_ondemand_price()
            .with_max_ondemand_price().with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=5760
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
    retries=0
)

###############################################################################
# steps
###############################################################################
rsm_data_generation_step = EmrJobTask(
    name="RSMAudienceModelInputGenerator",
    class_name="com.thetradedesk.audience.jobs.modelinput.AudienceModelInputGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[
        ('date', run_date),
        ('modelName', model),
        ('supportedDataSources', 'Seed,TTDOwnData'),
        ("supportedGraphs", "None"),
        ('saltToSplitDataset', 'RSM'),
        ('saltToSampleUserRSMSeed', '0BgGCE'),
        ("validateDatasetSplitModule", "5"),
        ("persistHoldoutSet", "true"),
        ("lastTouchNumberInBR", "3"),  # this can be set to 4
        ("bidImpressionLookBack", "0"),
        ("positiveSampleLowerThreshold", "200.0"),
        ("positiveSampleUpperThreshold", "20000.0"),
        ("negativeSampleRatio", "10"),
        ("userDownSampleHitPopulationRSMSeed", "100000"),
        ("seedCoalesceAfterFilter", "64"),
        ("recordIntermediateResult", "false"),
        ("labelMaxLength", "50"),
        ("seedSizeLowerScaleThreshold", "2"),
        ("seedSizeUpperScaleThreshold", "13"),
        ("IncrementalTrainingEnabled", "true"),
        ("trainingCadence", "100000"),
        ("IncrementalTrainingSampleRate", "0.2")
    ],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=8)
)

rsm_thresholds_generation_step = EmrJobTask(
    name="AudienceCalculateThresholdsJob",
    class_name="com.thetradedesk.audience.jobs.AudienceCalculateThresholdsJob",
    eldorado_config_option_pairs_list=[('date', run_date), ('modelName', model), ('IALFolder', ial_path)],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=8)
)

# s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/RSM/Seed_None/v=1/20240903000000/Full=Holdout/
env = TtdEnvFactory.get_from_system().execution_env
date_str = "{{ data_interval_start.strftime(\"%Y%m%d000000\") }}"
write_etl_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_etl_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{env}/audience/RSM/Seed_None/v=1/{date_str}/_ETL_SUCCESS",
        date="",
        append_file=False,
        dag=adag,
    )
)

write_etl_household_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_etl_household_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{env}/audience/RSM/Seed_IAV2Household/v=1/{date_str}/_ETL_SUCCESS",
        date="",
        append_file=False,
        dag=adag,
    )
)

write_etl_person_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_etl_person_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{env}/audience/RSM/Seed_IAV2Person/v=1/{date_str}/_ETL_SUCCESS",
        date="",
        append_file=False,
        dag=adag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

rsm_etl_cluster_task.add_parallel_body_task(rsm_data_generation_step)
rsm_etl_cluster_task.add_parallel_body_task(rsm_thresholds_generation_step)

# Flow
rsm_etl_dag >> dataset_sensor >> rsm_etl_cluster_task >> write_etl_success_file_task >> write_etl_household_success_file_task >> write_etl_person_success_file_task >> final_dag_status_step
