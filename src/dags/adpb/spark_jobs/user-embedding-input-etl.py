import copy
import logging
from datetime import datetime, timedelta

from airflow.operators.python import ShortCircuitOperator
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.datperf.datasets import geronimo_dataset
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = 'adpb-user-embedding-input-etl'
owner = ADPB.team

# Job configuration
jar_path = "s3://ttd-build-artefacts/user-embedding/dataforgein/jars/release/dataforgein.jar"
job_environment = TtdEnvFactory.get_from_system()
job_start_date = datetime(2024, 8, 7)
job_schedule_interval_in_hours = 24
job_schedule_interval = timedelta(hours=job_schedule_interval_in_hours)
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
training_days_of_week = [4]  # train on Friday, add other week days according to need

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_tags = owner.sub_team
    enable_slack_alert = True
else:
    slack_tags = None
    enable_slack_alert = False

cluster_tags = {
    "Team": owner.jira_team,
}
cluster_idle_timeout_seconds = 30 * 60

# Execution date
run_date = "{{ ds }}"  # run date should always be yesterday

# Compute
user_worker_cores = 8
user_num_workers = 400
user_num_partitions = 1 * user_worker_cores * user_num_workers

worker_cores = 48
num_workers = 220  # number of workers for user cluster(s)
num_partitions = 2 * worker_cores * num_workers
num_workers_ctx = 40  # number of workers for ctx cluster
num_partitions_ctx = 2 * worker_cores * num_workers_ctx

master_instance_types = [M5.m5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)]
worker_instance_types = [
    M5.m5_12xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M5a.m5a_12xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M6g.m6g_12xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M5.m5_24xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
    M5a.m5a_24xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2)
]

user_worker_instance_types = [
    R6g.r6g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
    R6g.r6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2)
]

# Application settings
java_settings_list = [
    ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"),
]

packages_tfrecord = [
    ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
]

spark_options_list = [
    # ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
    # ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.driver.maxResultSize=32G"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
]

spark_options_list_user_daily = copy.deepcopy(spark_options_list) + [
    ("conf", "spark.sql.shuffle.partitions=%s" % user_num_partitions),
    ("conf", "spark.default.parallelism=%s" % user_num_partitions),
    ("conf", "spark.sql.adaptive.enabled=true"),
    ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
]

spark_options_list_user = copy.deepcopy(spark_options_list) + [
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % num_partitions),
]

spark_options_list_ctx = copy.deepcopy(spark_options_list) + [
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions_ctx),
    ("conf", "spark.default.parallelism=%s" % num_partitions_ctx),
]

eldorado_option_list = [("date", run_date)]

application_configuration = [{
    'Classification': 'spark',
    'Properties': {
        'maximizeResourceAllocation': 'true'
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "50",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

# DAG
input_etl_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    depends_on_past=True,
    slack_tags=slack_tags,
    tags=[owner.jira_team],
    enable_slack_alert=enable_slack_alert,
    retries=0,
)
airflow_dag = input_etl_dag.airflow_dag

geronimo_bids_impressions_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[geronimo_dataset.with_check_type("hour")],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        task_id='geronimo_bids_impressions_check',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 6,  # wait up to 6 hours
    )
)


def check_is_training_day(run_date_str):
    run_date_weekday = datetime.strptime(run_date_str, "%Y-%m-%d").weekday()
    week_days = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    if run_date_weekday in training_days_of_week:
        logging.info(f"Run date {run_date_str} is {week_days[run_date_weekday]}, run weekly aggregation and modeling data generation tasks")
        return True
    else:
        logging.info(f"Run date {run_date_str} is {week_days[run_date_weekday]}, skip downstream tasks")
        return False


# check if it is training day, otherwise some tasks(join, explode & split) will be skipped
check_is_training_day_op_task = OpTask(
    op=ShortCircuitOperator(
        task_id="is_training_day",
        python_callable=check_is_training_day,
        op_kwargs={"run_date_str": "{{ ds }}"},
        dag=input_etl_dag.airflow_dag,
        trigger_rule="none_failed"
    )
)

user_daily_aggregation_cluster = EmrClusterTask(
    name="user-embedding-input-etl-user-aggregation-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=user_worker_instance_types,
        on_demand_weighted_capacity=user_num_workers,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment
)

ctx_daily_aggregation_cluster = EmrClusterTask(
    name="user-embedding-input-etl-ctx-aggregation-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=worker_instance_types,
        on_demand_weighted_capacity=num_workers_ctx,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment
)

weekly_aggregation_cluster = EmrClusterTask(
    name="user-embedding-input-etl-user-weekly-aggregation-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=worker_instance_types,
        on_demand_weighted_capacity=num_workers,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment
)

join_cluster = EmrClusterTask(
    name="user-embedding-input-etl-join-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=worker_instance_types,
        on_demand_weighted_capacity=num_workers,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment
)

# Steps

# Ctx Daily aggregation
ctx_daily_aggregation = EmrJobTask(
    name="CtxDailyAggregation",
    class_name="com.thetradedesk.dataforgein.jobs.CtxDailyAggregation",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_ctx,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=ctx_daily_aggregation_cluster.cluster_specs,
)

# ctx_daily_all_feature_generation = EmrJobTask(
#     name="CtxAllFeatureGeneration",
#     class_name="com.thetradedesk.dataforgein.jobs.CtxAllFeatureGeneration",
#     executable_path=jar_path,
#     additional_args_option_pairs_list=spark_options_list_ctx,
#     eldorado_config_option_pairs_list=eldorado_option_list,
#     timeout_timedelta=timedelta(hours=2),
#     cluster_specs=ctx_daily_aggregation_cluster.cluster_specs,
# )

# Ctx weekly aggregation
ctx_weekly_aggregation = EmrJobTask(
    name="CtxWeeklyAggregation",
    class_name="com.thetradedesk.dataforgein.jobs.CtxWeeklyAggregation",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_ctx,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=weekly_aggregation_cluster.cluster_specs,
    configure_cluster_automatically=True
)

# User daily aggregation
user_daily_aggregation = EmrJobTask(
    name="user-daily-aggregation",
    class_name="com.thetradedesk.dataforgein.jobs.UserDailyAggregation",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_user_daily,
    eldorado_config_option_pairs_list=eldorado_option_list + [
        ("cardinalityOutlierCappingThreshold", "350"),
        ("cardinalityDivide", "150"),
        ('skewFactor', "100"),
    ],
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=user_daily_aggregation_cluster.cluster_specs,
)

# User weekly aggregation
user_weekly_aggregation = EmrJobTask(
    name="user-weekly-aggregation",
    class_name="com.thetradedesk.dataforgein.jobs.UserWeeklyAggregation",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_user,
    eldorado_config_option_pairs_list=eldorado_option_list + [
        ("cardinalityOutlierCappingThreshold", "600"),
        ("cardinalityDivide", "200"),
    ],
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=weekly_aggregation_cluster.cluster_specs,
)

# User feature normalization
user_feature_normalization = EmrJobTask(
    name="user-feature-normalization",
    class_name="com.thetradedesk.dataforgein.jobs.UserFeatureNormalization",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_user,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=weekly_aggregation_cluster.cluster_specs,
)

# Positive & negative label selection
pos_neg_label_selection = EmrJobTask(
    name="pos-neg-label-selection",
    class_name="com.thetradedesk.dataforgein.jobs.PosNegLabelSelection",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_user,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=weekly_aggregation_cluster.cluster_specs,
    configure_cluster_automatically=True
)

join_user_ctx_label = EmrJobTask(
    name="join-user-ctx-label",
    class_name="com.thetradedesk.dataforgein.jobs.JoinUserCtxLabel",
    executable_path=jar_path,
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("conf", "spark.sql.shuffle.partitions=%s" % (3 * worker_cores * num_workers)),
        ("conf", "spark.default.parallelism=%s" % (3 * worker_cores * num_workers)),
        # ("conf", "spark.memory.offHeap.enabled=true"),
        # ("conf", "spark.memory.offHeap.size=86000m"),
        # ("conf", "spark.executor.memory=86000m"),
    ],
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=join_cluster.cluster_specs,
)

# Explode features and export datasets
generate_all_positive = EmrJobTask(
    name="generate-all-positive",
    class_name="com.thetradedesk.dataforgein.jobs.AllPositiveGeneration",
    executable_path=jar_path,
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list_user) + packages_tfrecord,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=join_cluster.cluster_specs,
    configure_cluster_automatically=False
)

generate_modeling_data = EmrJobTask(
    name="generate-modeling-data",
    class_name="com.thetradedesk.dataforgein.jobs.ModelingDataGeneration",
    executable_path=jar_path,
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list_user) + packages_tfrecord,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=join_cluster.cluster_specs,
    configure_cluster_automatically=False
)

data_quality_validation = EmrJobTask(
    name="data-quality-validation",
    class_name="com.thetradedesk.dataforgein.jobs.DataQualityValidation",
    executable_path=jar_path,
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list_user) + packages_tfrecord,
    eldorado_config_option_pairs_list=eldorado_option_list,
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=join_cluster.cluster_specs,
    configure_cluster_automatically=False
)

# Add steps to clusters
ctx_daily_aggregation_cluster.add_parallel_body_task(ctx_daily_aggregation)
# ctx_daily_aggregation_cluster.add_parallel_body_task(ctx_daily_all_feature_generation)
user_daily_aggregation_cluster.add_parallel_body_task(user_daily_aggregation)

weekly_aggregation_cluster.add_parallel_body_task(ctx_weekly_aggregation)
weekly_aggregation_cluster.add_parallel_body_task(user_weekly_aggregation)
weekly_aggregation_cluster.add_parallel_body_task(user_feature_normalization)
weekly_aggregation_cluster.add_parallel_body_task(pos_neg_label_selection)

join_cluster.add_parallel_body_task(join_user_ctx_label)
join_cluster.add_parallel_body_task(generate_modeling_data)
join_cluster.add_parallel_body_task(generate_all_positive)
join_cluster.add_parallel_body_task(data_quality_validation)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=input_etl_dag.airflow_dag))

# Dependencies
ctx_weekly_aggregation >> user_weekly_aggregation >> user_feature_normalization >> pos_neg_label_selection
join_user_ctx_label >> generate_modeling_data >> generate_all_positive >> data_quality_validation

input_etl_dag >> geronimo_bids_impressions_sensor_task

geronimo_bids_impressions_sensor_task >> user_daily_aggregation_cluster
geronimo_bids_impressions_sensor_task >> ctx_daily_aggregation_cluster

user_daily_aggregation_cluster >> check_is_training_day_op_task >> weekly_aggregation_cluster >> join_cluster >> final_dag_check
ctx_daily_aggregation_cluster >> check_is_training_day_op_task >> weekly_aggregation_cluster >> join_cluster >> final_dag_check
check_is_training_day_op_task >> final_dag_check
