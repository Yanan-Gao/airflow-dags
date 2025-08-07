import copy
from datetime import timedelta, datetime
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
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

run_date = "{{ data_interval_start.to_date_string() }}"
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
# if you change start_date, you need to take care circle_days_for_full_training in _decide_full_or_increment
start_date = datetime(2025, 5, 20, 2, 0)
circle_days_for_full_training = 100000
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3_2

rsmv2_training_data_measurement_dag = TtdDag(
    dag_id="perf-automation-rsmv2-trainng-data-status-measurement",
    start_date=start_date,
    schedule_interval=timedelta(hours=24),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO", "RSMV2"],
)

adag = rsmv2_training_data_measurement_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
policytable_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata",
    data_name="prod/audience/policyTable/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=False,
)

seed_none_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="prod/audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_ETL_SUCCESS",
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="data_available",
        datasets=[policytable_dataset, seed_none_etl_success_file],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 23,
    )
)

###############################################################################
# clusters
###############################################################################
rsmv2_training_data_measurement_full_cluster_task = EmrClusterTask(
    name="RSMV2_Training_Data_Measurement_Full_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(256).with_ebs_iops(16000).with_ebs_throughput(750).with_max_ondemand_price()
            .with_fleet_weighted_capacity(2)
        ],
        on_demand_weighted_capacity=50
    ),
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
    retries=0
)

rsmv2_training_data_measurement_incr_cluster_task = EmrClusterTask(
    name="RSMV2_Training_Data_Measurement_Incr_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(256).with_ebs_iops(16000).with_ebs_throughput(750).with_max_ondemand_price()
            .with_fleet_weighted_capacity(2)
        ],
        on_demand_weighted_capacity=50
    ),
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
    retries=0
)
###############################################################################
# steps
###############################################################################


def create_measurement_job_task(name, eldorado_config_specific_list) -> EmrJobTask:
    # common config
    eldorado_config_list = [
        ("date", run_date),
        ("AudienceModelPolicyReadableDatasetReadEnv", "prod"),
    ]
    eldorado_config_list.extend(eldorado_config_specific_list)
    return EmrJobTask(
        name=name,
        class_name="com.thetradedesk.audience.jobs.RSMV2TrainingDataStatusGeneratorJob",
        additional_args_option_pairs_list=(
            copy.deepcopy(spark_options_list) + [(
                "jars",
                "s3://thetradedesk-mlplatform-us-east-1/libs/common/spark_tfrecord_2_12_0_3_4-56ef7.jar",
            )]
        ),
        eldorado_config_option_pairs_list=eldorado_config_list,
        action_on_failure="CONTINUE",
        executable_path=AUDIENCE_JAR,
        timeout_timedelta=timedelta(hours=8),
    )


rsm_full_measurement_tasks_config = [
    {
        "name": "RSMV2_training_data_status_measurement_full_seed_no_sensitive_task",
        "config": [
            ("schedule", "Full"),
            ("subfolder", "SeedNoSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_full_seed_sensitive_task",
        "config": [
            ("schedule", "Full"),
            ("subfolder", "SeedSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_full_ttdown_no_sensitive_task",
        "config": [
            ("schedule", "Full"),
            ("subfolder", "TTDOwnDataNoSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_full_ttdown_sensitive_task",
        "config": [
            ("schedule", "Full"),
            ("subfolder", "TTDOwnDataSensitive"),
        ],
    },
]
rsm_incr_measurement_tasks_config = [
    {
        "name": "RSMV2_training_data_status_measurement_incr_seed_no_sensitive_small_task",
        "config": [
            ("schedule", "Small"),
            ("subfolder", "SeedNoSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_seed_sensitive_small_task",
        "config": [
            ("schedule", "Small"),
            ("subfolder", "SeedSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_ttdown_no_sensitive_small_task",
        "config": [
            ("schedule", "Small"),
            ("subfolder", "TTDOwnDataNoSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_ttdown_sensitive_small_task",
        "config": [
            ("schedule", "Small"),
            ("subfolder", "TTDOwnDataSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_seed_no_sensitive_new_task",
        "config": [
            ("schedule", "New"),
            ("subfolder", "SeedNoSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_seed_sensitive_new_task",
        "config": [
            ("schedule", "New"),
            ("subfolder", "SeedSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_ttdown_no_sensitive_new_task",
        "config": [
            ("schedule", "New"),
            ("subfolder", "TTDOwnDataNoSensitive"),
        ],
    },
    {
        "name": "RSMV2_training_data_status_measurement_incr_ttdown_sensitive_new_task",
        "config": [
            ("schedule", "New"),
            ("subfolder", "TTDOwnDataSensitive"),
        ],
    },
]

for cfg in rsm_full_measurement_tasks_config:
    task = create_measurement_job_task(cfg["name"], cfg["config"])
    rsmv2_training_data_measurement_full_cluster_task.add_parallel_body_task(task)

for cfg in rsm_incr_measurement_tasks_config:
    task = create_measurement_job_task(cfg["name"], cfg["config"])
    rsmv2_training_data_measurement_incr_cluster_task.add_parallel_body_task(task)


def set_step_concurrency(emr_cluster_task: EmrClusterTask, concurrency: int = 10) -> EmrClusterTask:
    job_flow = emr_cluster_task._setup_tasks.last_airflow_op().job_flow_overrides
    job_flow["StepConcurrencyLevel"] = concurrency

    for step in job_flow["Steps"]:
        step["ActionOnFailure"] = "CONTINUE"

    return emr_cluster_task


rsmv2_training_data_measurement_concurrent_full_cluster_task = set_step_concurrency(
    rsmv2_training_data_measurement_full_cluster_task, len(rsm_full_measurement_tasks_config)
)

rsmv2_training_data_measurement_concurrent_incr_cluster_task = set_step_concurrency(
    rsmv2_training_data_measurement_incr_cluster_task, len(rsm_incr_measurement_tasks_config)
)

env = TtdEnvFactory.get_from_system().execution_env
date_str = "{{ data_interval_start.strftime(\"%Y%m%d\") }}"
write_measurement_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_measurement_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{env}/audience/RSMV2/measurement/trainingMetaData/v=1/{date_str}/_TRAINING_STATUS_MEASUREMENT_SUCCESS",
        date="",
        append_file=False,
        dag=adag,
        trigger_rule="none_failed",
    )
)


def _decide_full_or_increment(**context):
    execution_date = context["data_interval_start"]
    dag_start_date = context['dag'].default_args['start_date']

    diff_days = (execution_date - dag_start_date).days
    remainder = diff_days % circle_days_for_full_training

    if remainder == 0:
        return "full_tasks"
    else:
        return "incr_tasks"


decide_full_or_increment = OpTask(
    op=BranchPythonOperator(task_id="decide_full_or_increment", python_callable=_decide_full_or_increment, provide_context=True)
)


def _run_full(**context):
    print("run full")


def _run_incr(**context):
    print("run inc")


full_tasks = OpTask(op=PythonOperator(task_id='full_tasks', python_callable=_run_full))

incr_tasks = OpTask(op=PythonOperator(task_id='incr_tasks', python_callable=_run_incr))

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
rsmv2_training_data_measurement_dag >> dataset_sensor >> decide_full_or_increment
decide_full_or_increment >> full_tasks >> rsmv2_training_data_measurement_concurrent_full_cluster_task >> write_measurement_success_file_task
decide_full_or_increment >> incr_tasks >> rsmv2_training_data_measurement_concurrent_incr_cluster_task >> write_measurement_success_file_task
write_measurement_success_file_task >> final_dag_status_step
