from __future__ import annotations

from datetime import datetime, timedelta
from typing import Tuple, Sequence, List

from dags.ctxmp import spark_log4j2, constants
from ttd.datasets.dataset import Dataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_name = 'contextual-relevance'
job_schedule_interval = timedelta(days=1)
job_start_date = datetime(2025, 5, 27, 0, 0)

env = TtdEnvFactory.get_from_system()

target_date = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
target_date_no_dash = "{{ data_interval_start.strftime(\"%Y%m%d\") }}"
relevance_num_partitions = 10
aerospike_write_enabled = env == TtdEnvFactory.prod

max_active_runs = 3 if env == TtdEnvFactory.prod else 1
run_only_latest = env != TtdEnvFactory.prod
cluster_task_retries = 1 if env == TtdEnvFactory.prod else 0

preBidResults3DayRootPath = "s3://ttd-contextual/prod/parquet/aggregated_prebid_results_3day/v=1"
referrercacheUrl2UserId1DayRootPath = "s3://ttd-contextual/prod/parquet/aggregated_referrercacheurl_userid_1day/v=2"

tdid2SeedIdRootPath = "s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/tdid2seedid/v=1"
seedIdRootPath = "s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/seedids/v=2"


def get_job_task(
    name: str, class_name: str, eldorado_args: Sequence[Tuple[str, str]], max_result_size_gb: int, parallelism: int,
    timeout_timedelta: timedelta
) -> EmrJobTask:
    return EmrJobTask(
        name=name,
        executable_path=constants.eldorado_jar,
        class_name=class_name,
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=eldorado_args,
        additional_args_option_pairs_list=[
            ("conf", f"spark.driver.maxResultSize={max_result_size_gb}g"),
            ("conf", f"spark.sql.shuffle.partitions={parallelism}"),
        ],
        timeout_timedelta=timeout_timedelta,
        retries=0
    )


def cluster_task(name: str, capacity: int, cluster_auto_terminates: bool = True) -> EmrClusterTask:
    cluster_tags = {
        "Team": slack_groups.CTXMP.team.jira_team,
    }

    return EmrClusterTask(
        name=name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M7g.m7g_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=default_core_fleet_instance_types(capacity),
            on_demand_weighted_capacity=capacity,
        ),
        emr_release_label=constants.aws_emr_version,
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=cluster_auto_terminates,
        cluster_tags=cluster_tags,
        additional_application_configurations=[spark_log4j2.get_emr_configuration()],
        retries=cluster_task_retries
    )


def get_date_dataset(path: str) -> Dataset:
    [bucket, prefix] = path.replace('s3://', '').split('/', 1)
    [path_prefix, version_str] = prefix.rsplit('/', 1)
    version = int(version_str.replace('v=', ''))
    return DateGeneratedDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name='',
        env_aware=False,
        version=version,
        date_format="date=%Y%m%d",
        success_file="_SUCCESS"
    )


def check_relevance_data_task(task_id: str):

    def get_tdid2seed_dataset(split: int) -> Dataset:
        return DateGeneratedDataset(
            bucket="thetradedesk-mlplatform-us-east-1",
            path_prefix="data/prod",
            env_aware=False,
            data_name="audience/scores/tdid2seedid",
            version=1,
            date_format=f"date=%Y%m%d/split={split}",
            success_file=None
        )

    return OpTask(
        op=DatasetCheckSensor(
            datasets=[get_tdid2seed_dataset(split) for split in range(0, 10)] + [get_date_dataset(seedIdRootPath)],
            ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
            task_id=task_id,
            poke_interval=60 * 10,  # poke every 10 minutes
            timeout=60 * 60 * 30,  # RSM dataset can be delayed up to 24 hours - give some leeway here
        )
    )


def check_daily_data_task(task_id: str, paths: Sequence[str], timeout: int) -> OpTask:
    return OpTask(
        op=DatasetCheckSensor(
            datasets=[get_date_dataset(p) for p in paths],
            ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
            task_id=task_id,
            poke_interval=60 * 10,  # poke every 10 minutes
            timeout=timeout,
        )
    )


###########################################
#   Cluster types
###########################################
def default_core_fleet_instance_types(capacity: int) -> List[EmrInstanceType]:
    _16xlarge = [
        R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
    ]
    _12xlarge = [
        R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
    ]
    _8xlarge = [
        R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
        R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    ]
    _4xlarge = [
        R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
        R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    ]

    if capacity < 16:
        raise Exception(f'Capacity must be 16 or above, but is {capacity}')
    elif capacity > 1000:
        return _12xlarge + _16xlarge
    else:
        return _4xlarge + _8xlarge


###########################################
#   DAG setup
###########################################
dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=max_active_runs,
    slack_tags=slack_groups.CTXMP.team.name,
    slack_channel=slack_groups.CTXMP.team.alarm_channel,
    enable_slack_alert=False,
    run_only_latest=run_only_latest
)

check_contextual_data = check_daily_data_task(
    'CheckContextualDataPresence', [preBidResults3DayRootPath, referrercacheUrl2UserId1DayRootPath], timeout=60 * 60 * 12
)

user_category_calculation = cluster_task(name='UserCategoryJob', capacity=2000)
user_category_calculation.add_parallel_body_task(
    get_job_task(
        name='UserCategoryJob',
        class_name='jobs.contextual.relevance.UserCategoryMappingJob',
        eldorado_args=[('targetDate', target_date), ('maxNumUsersPerSegment', '10000')],
        max_result_size_gb=10,
        parallelism=4000,
        timeout_timedelta=timedelta(hours=2)
    )
)

check_relevance_data = check_relevance_data_task('CheckRelevanceDataPresence')

contextual_relevance_calculation = cluster_task(name='ContextualRelevanceCalculation', capacity=3000, cluster_auto_terminates=False)
for partition in range(0, relevance_num_partitions):
    contextual_relevance_calculation.add_sequential_body_task(
        get_job_task(
            name=f'ContextualRelevanceCalculation_{partition}',
            class_name='jobs.contextual.relevance.ContextualRelevanceJob',
            eldorado_args=[('targetDate', target_date), ('partition', str(partition)), ('partitionCount', str(relevance_num_partitions)),
                           ('tdid2SeedIdPath', f'{tdid2SeedIdRootPath}/date={target_date_no_dash}'),
                           ('seedIdPath', f'{seedIdRootPath}/date={target_date_no_dash}'), ('userIdCountThreshold', '0'),
                           ('ttd.ds.UserCategoryDataset.isInChain', 'true')],
            max_result_size_gb=4,
            parallelism=6000,
            timeout_timedelta=timedelta(minutes=30)
        )
    )

update_aerospike = cluster_task(name='UpdateAerospike', capacity=300)
update_aerospike.add_parallel_body_task(
    get_job_task(
        name='UpdateAerospike',
        class_name='jobs.contextual.relevance.RelevanceAerospikeUpdaterJob',
        eldorado_args=[
            ('targetDate', target_date),
            ('aerospikeHost', "{{ macros.ttd_extras.resolve_consul_url('ttd-ctxmp-rsm.aerospike.service.useast.consul', port=3000) }}"),
            ('aerospikeWriteEnabled', 'true' if aerospike_write_enabled else 'false'), ('minUserIdCount', '100')
        ],
        max_result_size_gb=4,
        parallelism=500,
        timeout_timedelta=timedelta(hours=1)
    )
)

dag >> check_contextual_data >> user_category_calculation >> check_relevance_data >> contextual_relevance_calculation
contextual_relevance_calculation >> update_aerospike

globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
