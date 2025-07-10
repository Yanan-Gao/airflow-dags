import copy
from datetime import timedelta, datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
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
spark_options_list = [("executor-memory", "16G"), ("executor-cores", "4"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=5G"),
                      ("conf", "spark.driver.maxResultSize=5G")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

DATE_TIME = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"

ADVERTISER_SURVEY_JAR = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar"

# Define Dag
# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
advertiser_survey_etl = TtdDag(
    dag_id="perf-automation-advertiser-survey-etl",
    start_date=datetime(year=2024, month=9, day=27, hour=0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/yrMMCQ',
    retries=0,
    max_active_runs=4,
    retry_delay=timedelta(hours=1),
    tags=['DATPERF'],
    enable_slack_alert=False
)

advertiser_survey_dag = advertiser_survey_etl.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
advertiser_survey_results_dataset = DateGeneratedDataset(
    bucket="ttd-advertiseronboarding",
    path_prefix="prod/ExportAdvertiserOnboardingDataToS3",
    data_name="Default",
    version=None,
    env_aware=False,
)

###############################################################################
# S3 dataset sensors
###############################################################################
advertiser_survey_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='advertiser_survey_data_available',
        datasets=[advertiser_survey_results_dataset],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 3,
    )
)

###############################################################################
# clusters
###############################################################################
cores = 64

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_xlarge().with_fleet_weighted_capacity(16),
    ],
    on_demand_weighted_capacity=cores,
)

advertiser_survey_etl_cluster = EmrClusterTask(
    name="AdvertiserSurveyEtlCluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={'Team': DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

advertiser_survey_etl_step = EmrJobTask(
    name="GenerateAdvertiserSurveyBidAdjustmentsParquet",
    class_name="com.thetradedesk.jobs.advertisersurvey.AdvertiserSurveyETL",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=[('date', DATE_TIME), ('partitions', cores * 2)],
    executable_path=ADVERTISER_SURVEY_JAR,
    timeout_timedelta=timedelta(hours=4)
)

# add step to cluster
advertiser_survey_etl_cluster.add_parallel_body_task(advertiser_survey_etl_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=advertiser_survey_dag))

# Flow
advertiser_survey_etl >> advertiser_survey_dataset_sensor >> advertiser_survey_etl_cluster >> final_dag_status_step
