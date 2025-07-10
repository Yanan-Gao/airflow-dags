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
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask

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

model = "AEM"
run_date = "{{ data_interval_start.to_date_string() }}"
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/yxz-AUDAUTO-2179-erm-graph-policy/latest/audience.jar"

erm_etl_dag = TtdDag(
    dag_id="perf-automation-erm-etl",
    start_date=datetime(2024, 7, 20, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "ERM"]
)

adag = erm_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
policytable_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata",
    data_name="audience/policyTable/AEM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=True,
)

aggregatedconversionpixel_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="audience/aggregatedConversionPixel",
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
        datasets=[policytable_dataset, aggregatedconversionpixel_dataset, bidsimpressions_data],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

###############################################################################
# clusters
###############################################################################
erm_etl_cluster_task = EmrClusterTask(
    name="ERM_ETL_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32)],
        on_demand_weighted_capacity=2080
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

###############################################################################
# steps
###############################################################################
erm_data_generation_step = EmrJobTask(
    name="ERMAudienceModelInputGenerator",
    class_name="com.thetradedesk.audience.jobs.modelinput.AudienceModelInputGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[
        ('date', run_date),
        ('modelName', model),
        ('supportedDataSources', 'Conversion'),
        ('saltToSplitDataset', model),
        ('saltToSampleUserAEMConversion', '0BgGCE'),
        ("validateDatasetSplitModule", "5"),
        ("persistHoldoutSet", "true"),
        ("lastTouchNumberInBR", "3"),  # this can be set to 4
        ("bidImpressionLookBack", "0"),
        ("positiveSampleLowerThreshold", "500.0"),
        ("positiveSampleUpperThreshold", "50000.0"),
        ("negativeSampleRatio", "10"),
        ("userDownSampleHitPopulationAEMConversion", "200000"),
        ("recordIntermediateResult", "false"),
        ("labelMaxLength", "50"),
        ("seedSizeLowerScaleThreshold", "1"),
        ("seedSizeUpperScaleThreshold", "13"),
        ("featuresJsonPath", "'s3://thetradedesk-mlplatform-us-east-1/features/data/AEM/v=1/prod/schemas/features.json'")
    ],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=8)
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

erm_etl_cluster_task.add_parallel_body_task(erm_data_generation_step)

# Flow
erm_etl_dag >> dataset_sensor >> erm_etl_cluster_task >> final_dag_status_step
