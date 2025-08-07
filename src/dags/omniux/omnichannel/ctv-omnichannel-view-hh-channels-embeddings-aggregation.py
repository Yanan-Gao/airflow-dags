import logging

from airflow.operators.python import ShortCircuitOperator

from datetime import datetime, timedelta, date
from datasources.datasources import Datasources
from dags.omniux.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

user_channels_aggregation_job_name = "ctv-omnichannel-view-hh-channels-embeddings-aggregation"
start_date = datetime(2024, 8, 23, 0, 0)
env = TtdEnvFactory.get_from_system()
check_embedding_version_task_id = "check_and_push_embedding_version"
user_embedding_key = "user_embedding_version"
user_embedding_version = (
    f"task_instance.xcom_pull("
    f"dag_id='{user_channels_aggregation_job_name}', "
    f"task_ids='{check_embedding_version_task_id}', "
    f"key='{user_embedding_key}')"
)

user_channels_aggregation_dag = TtdDag(
    dag_id=user_channels_aggregation_job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    enable_slack_alert=True,
    tags=[OMNIUX.team.name, "OmnichannelView"],
    run_only_latest=True
)

dag = user_channels_aggregation_dag.airflow_dag


def check_and_push_embedding_version(**kwargs) -> bool:
    """
    Checks latest UserEmbedding version against latest HHChannelsEmbeddings processed date. If UserEmbedding version
    is greater, push UserEmbedding version and return True. Otherwise, return False.
    """
    run_date = kwargs['data_interval_end']
    formatted_run_date = date(run_date.year, run_date.month, run_date.day)

    most_recent_processed_date = Datasources.ctv.household_channels_embeddings(env.dataset_read_env) \
        .check_recent_data_exist(
        cloud_storage=CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build(),
        ds_date=formatted_run_date,
        max_lookback=30
    )

    if not most_recent_processed_date:
        raise ValueError("Error fetching HHChannelsEmbeddings Dataset date")
    logging.info(f"Most recent processed HHChannelsEmbeddings is {most_recent_processed_date.get()}")

    most_recent_user_embedding_date = Datasources.ctv.user_embeddings \
        .check_recent_data_exist(
            cloud_storage=CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build(),
            ds_date=formatted_run_date,
            max_lookback=30
        )

    if not most_recent_user_embedding_date:
        raise ValueError("Error fetching UserEmbeddings Dataset date")
    logging.info(f"Most recent processed UserEmbeddings is {most_recent_user_embedding_date.get()}")

    if most_recent_user_embedding_date.get() > most_recent_processed_date.get():
        kwargs['task_instance'].xcom_push(key=user_embedding_key, value=most_recent_user_embedding_date.get())
        return True

    return False


check_embedding_version_task = OpTask(
    op=ShortCircuitOperator(
        task_id=check_embedding_version_task_id, python_callable=check_and_push_embedding_version, dag=dag, provide_context=True
    )
)

check_hhs_avails_exist = OpTask(
    op=DatasetCheckSensor(
        datasets=[Datasources.avails.household_sampled_identity_deal_agg_hourly("high", "openGraphAdBrain").with_check_type("day")],
        ds_date='{{' + user_embedding_version + '.strftime("%Y-%m-%d %H:%M:%S") }}',
        lookback=7,
        poke_interval=60 * 10,
        generate_task_id=True,
        dag=dag
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=20
)

cluster_task = EmrClusterTask(
    name=user_channels_aggregation_job_name + "-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": OMNIUX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name=user_channels_aggregation_job_name + "-job-task",
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.omnichannelview.HouseholdChannelsEmbeddingsAggregation",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ('lookback', '7'),
        ('date', '{{' + user_embedding_version + '.strftime("%Y-%m-%d") }}'),
        ('omnichannelViewPath', 's3://ttd-ctv/'),
    ]
)

cluster_task.add_parallel_body_task(job_task)

user_channels_aggregation_dag >> check_embedding_version_task >> check_hhs_avails_exist >> cluster_task
