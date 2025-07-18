import copy
from datetime import timedelta, datetime

from airflow.operators.python import BranchPythonOperator, PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.confetti.confetti_task_factory import make_confetti_tasks, resolve_env
from ttd.eldorado.xcom.helpers import get_xcom_pull_jinja_string
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

run_date = "{{ data_interval_start.to_date_string() }}"
PROD_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
TEST_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"

experiment = "yanan-demo"
experiment_suffix = f"/experiment={experiment}" if experiment else ""

AUDIENCE_JAR = PROD_JAR if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else TEST_JAR

# if you change start_date, you need to take care circle_days_for_full_training in _decide_full_or_increment
start_date = datetime(2025, 5, 25, 2, 0)
circle_days_for_full_training = 100000
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3_2
env = TtdEnvFactory.get_from_system().execution_env
override_env = f"test{experiment_suffix}" if env == "prodTest" else env  # only apply experiment suffix in prodTest
policy_table_read_env = "prod"
feature_store_read_env = "prod"

environment = TtdEnvFactory.get_from_system()
confetti_env = resolve_env(env, experiment)

rsmv2_etl_dag = TtdDag(
    dag_id="perf-automation-rsmv2-etl",
    start_date=start_date,
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "RSMV2"]
)

adag = rsmv2_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
policytable_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata/prod",
    data_name="audience/policyTable/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=False,  # always read prod
)

aggregatedseed_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/aggregatedSeed",
    version=1,
    env_aware=False,  # always read prod
)

base_feature_path = f"features/feature_store/{feature_store_read_env}"
job_prefix = ("profiles/"
              "source=bidsimpression/"
              "index=TDID/"
              "job=DailyTDIDDensityScoreSplitJobSub/")
date_fmt = "date=%Y%m%d/split=0"

# define (Source, FeatureKey)
feature_configs = [
    ("TTDOwnData", "AliasedSupplyPublisherIdCity"),
    ("TTDOwnData", "SiteZip"),
    ("Seed", "AliasedSupplyPublisherIdCity"),
    ("Seed", "SiteZip"),
]

featurestore_datasets = [
    DateGeneratedDataset(
        bucket="thetradedesk-mlplatform-us-east-1",
        path_prefix=base_feature_path,
        data_name=f"{job_prefix}Source={src}/FeatureKey={key}",
        version=1,
        date_format=date_fmt,
        env_aware=False,
    ) for src, key in feature_configs
]

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
        datasets=[policytable_dataset, aggregatedseed_dataset, bidsimpressions_data, *featurestore_datasets],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 23,
    )
)

###############################################################################
# clusters
###############################################################################
rsmv2_etl_full_cluster_task = EmrClusterTask(
    name="RSMV2_ETL_Full_Cluster",
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
            .with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=5760
    ),
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
    retries=0
)

rsmv2_etl_inc_cluster_task = EmrClusterTask(
    name="RSMV2_ETL_Inc_Cluster",
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
            .with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=5760
    ),
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
    retries=0
)

# Prepare Confetti runtime config once per cluster
prep_confetti_full, gate_confetti_full = make_confetti_tasks(
    group_name="audience",
    job_name="RelevanceModelInputGeneratorJob",
    experiment_name=experiment,
    run_date=run_date,
    task_id_prefix="full_",
)

prep_confetti_inc, gate_confetti_inc = make_confetti_tasks(
    group_name="audience",
    job_name="RelevanceModelInputGeneratorJob",
    experiment_name=experiment,
    run_date=run_date,
    task_id_prefix="inc_",
)

dataset_sensor >> prep_confetti_full >> gate_confetti_full >> rsmv2_etl_full_cluster_task
dataset_sensor >> prep_confetti_inc >> gate_confetti_inc >> rsmv2_etl_inc_cluster_task

###############################################################################
# steps
###############################################################################
short_date_str = "{{ data_interval_start.strftime(\"%Y%m%d\") }}"
ial_path = f"s3://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date={short_date_str}"


def create_rsm_threshold_task(prefix):
    rsm_thresholds_generation_step = EmrJobTask(
        name=f"{prefix}AudienceCalculateThresholdsJob",
        class_name="com.thetradedesk.audience.jobs.AudienceCalculateThresholdsJob",
        eldorado_config_option_pairs_list=[('date', run_date), ('modelName', "RSMV2"), ('IALFolder', ial_path),
                                           ('AudienceModelPolicyReadableDatasetReadEnv', policy_table_read_env),
                                           ("ttdWriteEnv", override_env)],
        action_on_failure="CONTINUE",
        executable_path=AUDIENCE_JAR,
        timeout_timedelta=timedelta(hours=8)
    )
    return rsm_thresholds_generation_step


def create_rsm_job_task(name, eldorado_config_specific_list, prep_task, gate_task):
    # common config
    eldorado_config_list = [('date', run_date), ('optInSeedType', 'Dynamic'),
                            ('AudienceModelPolicyReadableDatasetReadEnv', policy_table_read_env),
                            ('AggregatedSeedReadableDatasetReadEnv', policy_table_read_env),
                            ('FeatureStoreReadEnv', feature_store_read_env), ('posNegRatio', '50'), ("ttdWriteEnv", override_env)]
    eldorado_config_list.extend(eldorado_config_specific_list)

    job_task = EmrJobTask(
        name=name,
        class_name="com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJob",
        additional_args_option_pairs_list=(
            copy.deepcopy(spark_options_list) +
            [("jars", "s3://thetradedesk-mlplatform-us-east-1/libs/common/spark_tfrecord_2_12_0_3_4-56ef7.jar")]
        ),
        eldorado_config_option_pairs_list=eldorado_config_list +
        [("confettiEnv", confetti_env), ("experimentName", experiment),
         (
             "confettiRuntimeConfigBasePath",
             get_xcom_pull_jinja_string(task_ids=prep_task.task_id, key="confetti_runtime_config_base_path"),
         )],
        action_on_failure="CONTINUE",
        executable_path=get_xcom_pull_jinja_string(task_ids=prep_task.task_id, key="audienceJarPath"),
        timeout_timedelta=timedelta(hours=8),
    )

    prep_task >> gate_task >> job_task
    return job_task


featureReadPathPrefix = "profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJobSub"

rsm_full_tasks_config = [
    {
        "name":
        "RelevanceModelInputGeneratorTTDOwnDataSensitiveFull",
        "config":
        [('optInSeedFilterExpr', "'Source=6 and SyntheticId IN (171374, 197580)'"), ("subFolder", "TTDOwnDataSensitiveFull"),
         ("sensitiveFeatureColumn", "'Site,Zip'"), ("upLimitPosCntPerSeed", "2000000"),
         ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=TTDOwnData/FeatureKey=AliasedSupplyPublisherIdCity/v=1'")]
    },
    {
        "name":
        "RelevanceModelInputGeneratorSeedSensitiveFull",
        "config": [
            ('optInSeedFilterExpr', "'Source=3 and IsSensitive'"), ("subFolder", "SeedSensitiveFull"),
            ("sensitiveFeatureColumn", "'Site,Zip'"),
            ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=Seed/FeatureKey=AliasedSupplyPublisherIdCity/v=1'")
        ]
    },
    {
        "name":
        "RelevanceModelInputGeneratorTTDOwnDataNonsensitiveFull",
        "config": [('optInSeedFilterExpr', "'Source=6'"), ("subFolder", "TTDOwnDataNonsensitiveFull"),
                   ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=TTDOwnData/FeatureKey=SiteZip/v=1'")]
    },
    {
        "name":
        "RelevanceModelInputGeneratorSeedNonsensitiveFull",
        "config": [('optInSeedFilterExpr', "'Source=3 and not IsSensitive'"), ("subFolder", "SeedNonsensitiveFull"),
                   ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=Seed/FeatureKey=SiteZip/v=1'")]
    },
]
rsm_inc_tasks_config = [
    {
        "name":
        "RelevanceModelInputGeneratorTTDOwnDataSensitiveSmall",
        "config":
        [('optInSeedFilterExpr', "'Source=6 and SyntheticId IN (171374, 197580)'"), ("subFolder", "TTDOwnDataSensitiveSmall"),
         ("sensitiveFeatureColumn", "'Site,Zip'"),
         ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=TTDOwnData/FeatureKey=AliasedSupplyPublisherIdCity/v=1'"),
         ("upLimitPosCntPerSeed", "500000")]
    },
    {
        "name":
        "RelevanceModelInputGeneratorTTDOwnDataNonsensitiveSmall",
        "config": [('optInSeedFilterExpr', "'Source=6 and Tag=4'"), ("subFolder", "TTDOwnDataNonsensitiveSmall"),
                   ("upLimitPosCntPerSeed", "10000"),
                   ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=TTDOwnData/FeatureKey=SiteZip/v=1'")]
    },
    {
        "name":
        "RelevanceModelInputGeneratorSeedSensitiveSmall",
        "config":
        [('optInSeedFilterExpr', "'Source=3 and Tag=4 and IsSensitive'"), ("subFolder", "SeedSensitiveSmall"),
         ("upLimitPosCntPerSeed", "10000"), ("sensitiveFeatureColumn", "'Site,Zip'"),
         ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=Seed/FeatureKey=AliasedSupplyPublisherIdCity/v=1'")]
    },
    {
        "name":
        "RelevanceModelInputGeneratorSeedNonsensitiveSmall",
        "config": [('optInSeedFilterExpr', "'Source=3 and Tag=4 and not IsSensitive'"), ("subFolder", "SeedNonsensitiveSmall"),
                   ("upLimitPosCntPerSeed", "10000"),
                   ("densityFeatureReadPathWithoutSlash", f"'{featureReadPathPrefix}/Source=Seed/FeatureKey=SiteZip/v=1'")]
    },
]

for cfg in rsm_full_tasks_config:
    job = create_rsm_job_task(cfg["name"], cfg["config"], prep_confetti_full, gate_confetti_full)
    rsmv2_etl_full_cluster_task.add_parallel_body_task(job)

# rsmv2_etl_full_cluster_task.add_parallel_body_task(create_rsm_threshold_task("Full"))

for cfg in rsm_inc_tasks_config:
    job = create_rsm_job_task(cfg["name"], cfg["config"], prep_confetti_inc, gate_confetti_inc)
    rsmv2_etl_inc_cluster_task.add_parallel_body_task(job)

# rsmv2_etl_inc_cluster_task.add_parallel_body_task(create_rsm_threshold_task("Incremental"))


def set_step_concurrency(emr_cluster_task: EmrClusterTask, concurrency: int = 10) -> EmrClusterTask:
    job_flow = emr_cluster_task._setup_tasks.last_airflow_op().job_flow_overrides
    job_flow["StepConcurrencyLevel"] = concurrency

    for step in job_flow["Steps"]:
        step["ActionOnFailure"] = "CONTINUE"

    return emr_cluster_task


rsmv2_etl_concurrent_full_cluster_task = set_step_concurrency(rsmv2_etl_full_cluster_task, len(rsm_full_tasks_config))

rsmv2_etl_concurrent_inc_cluster_task = set_step_concurrency(rsmv2_etl_inc_cluster_task, len(rsm_inc_tasks_config))

date_str = "{{ data_interval_start.strftime(\"%Y%m%d000000\") }}"
write_etl_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_etl_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_ETL_SUCCESS",
        date="",
        append_file=False,
        dag=adag,
        trigger_rule='none_failed'
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
        return "inc_tasks"


decide_full_or_increment = OpTask(
    op=BranchPythonOperator(task_id="decide_full_or_increment", python_callable=_decide_full_or_increment, provide_context=True)
)


def _run_full(**context):
    print("run full")


def _run_inc(**context):
    print("run inc")


full_tasks = OpTask(op=PythonOperator(task_id='full_tasks', python_callable=_run_full))

inc_tasks = OpTask(op=PythonOperator(task_id='inc_tasks', python_callable=_run_inc))

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
rsmv2_etl_dag >> dataset_sensor >> decide_full_or_increment
decide_full_or_increment >> full_tasks >> rsmv2_etl_concurrent_full_cluster_task >> write_etl_success_file_task
decide_full_or_increment >> inc_tasks >> rsmv2_etl_concurrent_inc_cluster_task >> write_etl_success_file_task
write_etl_success_file_task >> final_dag_status_step
