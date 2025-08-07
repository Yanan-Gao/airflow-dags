# flake8: noqa: F541
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from dags.datperf.datasets import geronimo_dataset

import copy

# Philo version setup, always remember to change for dev and prod
MODEL_VERSION = 6
GERONIMO_VERSION = 1
java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [
    ("executor-memory", "202G"),
    ("executor-cores", "32"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=110G"),
    ("conf", "spark.driver.cores=15"),
    ("conf", "spark.sql.shuffle.partitions=5000"),
    ("conf", "spark.driver.maxResultSize=50G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.memory.storageFraction=0.25"),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    },
}]

# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
DATE_MACRO = (f'{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}')
DEFAULT_YEAR = f'{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y") }}'
DEFAULT_MONTH = f'{{ (data_interval_start + macros.timedelta(days=1)).strftime("%m") }}'
DEFAULT_DAY = f'{{ (data_interval_start + macros.timedelta(days=1)).strftime("%d") }}'

LASTPARTITION = 23

# default values used on a dev run.
DEFAULT_PHILO_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/jars/prod/philo.jar"
DEFAULT_FEATURES_PATH = f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v={MODEL_VERSION}/prod/schemas/features.json"
DEFAULT_OUTPUT_PATH = f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v={MODEL_VERSION}"
run_only_latest = None  # set to None if you don't want to backfill and False if you want to backfill in dev env otherwise None
# parse args from manual trigger, if any
PHILO_JAR = f'{{{{ dag_run.conf.get("philo_jar") if dag_run.conf is not none and dag_run.conf.get("philo_jar") is not none else "{DEFAULT_PHILO_JAR}" }}}}'
FEATURES_PATH = f'{{{{ dag_run.conf.get("features_path") if dag_run.conf is not none and dag_run.conf.get("features_path") is not none else "{DEFAULT_FEATURES_PATH}" }}}}'

START_DATE = f'{{{{ dag_run.conf.get("start_date") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%Y-%m-%d") }}}}'

OUTPUT_PATH = f'{{{{ dag_run.conf.get("output_path") if dag_run.conf is not none and dag_run.conf.get("output_path") is not none else "{DEFAULT_OUTPUT_PATH}" }}}}'

# if we get a date, we need y/m/d to properly check for upstream datasets (philo etl in this case)
YEAR = f'{{{{ macros.datetime.strptime(dag_run.conf.get("start_date"), "%Y-%m-%d").strftime("%Y") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%Y") }}}}'

MONTH = f'{{{{ macros.datetime.strptime(dag_run.conf.get("start_date"), "%Y-%m-%d").strftime("%m") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%m") }}}}'

DAY = f'{{{{ macros.datetime.strptime(dag_run.conf.get("start_date"), "%Y-%m-%d").strftime("%d") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%d") }}}}'
# tfRecord Package
TFRECORD_PACKAGE = "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"
####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
philo_train = TtdDag(
    dag_id="perf-automation-philo-etl-sensitive",
    start_date=datetime(2025, 5, 22),
    schedule_interval=timedelta(days=1),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/yJLqCg",
    retries=0,
    max_active_runs=2,
    retry_delay=timedelta(hours=1),
    slack_channel="#scrum-perf-automation-alerts",
    default_args={"owner": "DATPERF"},
    slack_alert_only_for_prod=True,
    tags=["DATPERF", "Philo"],
    run_only_latest=run_only_latest,
)

dag = philo_train.airflow_dag

####################################################################################################################
# S3 dataset sensors
####################################################################################################################

geronimo_etl_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id="geronimo_etl_available",
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
        ds_date='{{ data_interval_start.strftime("%Y-%m-%d 23:00:00") }}',
        datasets=[geronimo_dataset],
    )
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
        R5.r5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5.r5_24xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(96),
    ],
    on_demand_weighted_capacity=1920,
)

cluster = EmrClusterTask(
    name="PhiloEtlClusterSensitive",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
)

####################################################################################################################
# steps
####################################################################################################################
# Default args

philo_adgroup_global_model_input_step = EmrJobTask(
    name="AdGroupGlobalModelInput",
    class_name="job.ModelInput",
    timeout_timedelta=timedelta(hours=6),
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", TFRECORD_PACKAGE),
    ],
    eldorado_config_option_pairs_list=java_settings_list + [
        ("date", START_DATE),
        ("roiFilter", "true"),
        ("outputPath", OUTPUT_PATH),
        ("featuresJson", FEATURES_PATH),
        ("outputPrefix", "global"),
        ("advertiserFilter", "true"),
        ("ttd.env", "prod"),
        ("separateSensitiveAdvertisers", "true"),
    ],
    executable_path=PHILO_JAR
)

cluster.add_parallel_body_task(philo_adgroup_global_model_input_step)

# philo_model_input_step = EmrJobTask(
#     name="ModelInputstep",
#     class_name="job.ModelInput",
#     timeout_timedelta=timedelta(hours=6),
#     additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
#         ("packages", TFRECORD_PACKAGE),
#     ],
#     eldorado_config_option_pairs_list=java_settings_list + [('date', START_DATE), ('roiFilter', 'false'), ('outputPath', OUTPUT_PATH),
#                                                             ('featuresJson', FEATURES_PATH), ('outputPrefix', 'processed'),
#                                                             ('partitions', 2000)],
#     executable_path=PHILO_JAR
# )

# cluster.add_parallel_body_task(philo_model_input_step)

final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)
# DAG dependencies
philo_train >> geronimo_etl_sensor >> cluster
cluster.last_airflow_op() >> final_dag_status_step
