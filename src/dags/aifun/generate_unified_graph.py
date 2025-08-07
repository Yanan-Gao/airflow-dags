from airflow.operators.python import PythonOperator

from ttd.cloud_provider import CloudProviders
from datetime import datetime, timedelta

from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from datasources.datasources import Datasources
import logging

from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d

from ttd.slack import slack_groups
from ttd.slack.slack_groups import AIFUN
from ttd.tasks.op import OpTask

from ttd.identity_graphs.identity_graphs import IdentityGraphs

FORECAST_JAR_PATH = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"

GENERATE_UNIFIED_GRAPH_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.GenerateUnifiedGraphJob"

iav2_graph_date_key = "iav2_graph_date"
iav2_open_graph_date_key = "iav2_open_graph_date"
adbrain_graph_date_key = "adbrain_graph_date"
adbrain_open_graph_date_key = "adbrain_open_graph_date"
singleton_graph_date_key = "singleton_graph_date"
graph_folder_name = "unified-open-graph"

dag_id = "generate-unified-graph-dag"
calculate_dates_task_id = "calculate_graph_dates_task_id"
job_schedule_interval = timedelta(days=7)
job_start_date = datetime(2024, 6, 24, 23, 0, 0)  # UTC 23 Mon means PST 15 Mon

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=AIFUN.team.alarm_channel,
    slack_tags=AIFUN.team.jira_team,
    enable_slack_alert=False,
    tags=[AIFUN.team.jira_team, "graph"],
    max_active_runs=2,
)

airflow_dag = dag.airflow_dag

cluster_name = "generate-unified-graph-cluster"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_xlarge().with_fleet_weighted_capacity(1),
        R5.r5_2xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_4xlarge().with_fleet_weighted_capacity(2),
        R5d.r5d_8xlarge().with_fleet_weighted_capacity(4),
    ],
    on_demand_weighted_capacity=80,
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

cluster_task = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.AIFUN.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label="emr-6.7.0",
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations],
)


def xcom_template_to_get_value(key: str) -> str:
    global dag_id, calculate_dates_task_id
    return (f"{{{{ "
            f'task_instance.xcom_pull(dag_id="{dag_id}", '
            f'task_ids="{calculate_dates_task_id}", '
            f'key="{key}") '
            f"}}}}")


additional_args_option_pairs_list = [("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

generate_unified_graph_task = EmrJobTask(
    name="gen-unified-graph",
    class_name=GENERATE_UNIFIED_GRAPH_CLASS,
    executable_path=FORECAST_JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("iav2Date", xcom_template_to_get_value(iav2_graph_date_key)),
        ("adbrainDate", xcom_template_to_get_value(adbrain_graph_date_key)),
        ("ogIav2Date", xcom_template_to_get_value(iav2_open_graph_date_key)),
        ("ogAdbrainDate", xcom_template_to_get_value(adbrain_open_graph_date_key)),
        ("singletonDate", xcom_template_to_get_value(singleton_graph_date_key)),
        ("graphFolderName", graph_folder_name),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

(generate_unified_graph_task)

cluster_task.add_parallel_body_task(generate_unified_graph_task)


def calculate_dates(**context):
    max_lookback_days = 21
    run_date = context["data_interval_end"].date()  # ds points to start of the interval, this points to end (datetime)
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    task_instance = context["task_instance"]

    identity_graphs = IdentityGraphs()
    identity_alliance = identity_graphs.identity_alliance
    most_recent_iav2_graph_date = identity_alliance.v2.based_on_ttd_graph_v1.default_client.dataset.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=run_date, max_lookback=max_lookback_days
    ).get()
    logging.info(f'Using iav2 legacy graph date {most_recent_iav2_graph_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=iav2_graph_date_key, value=most_recent_iav2_graph_date.strftime("%Y-%m-%d"))

    most_recent_iav2_open_graph_date = identity_alliance.v2.based_on_ttd_graph_v2.default_client.dataset.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=most_recent_iav2_graph_date, max_lookback=max_lookback_days
    ).get()
    logging.info(f'Using iav2 open graph date {most_recent_iav2_open_graph_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=iav2_open_graph_date_key, value=most_recent_iav2_open_graph_date.strftime("%Y-%m-%d"))

    ttd_graph = identity_graphs.ttd_graph
    most_recent_adbrain_graph_date = (
        ttd_graph.v1.default_client.dataset.check_recent_data_exist(
            cloud_storage=cloud_storage,
            ds_date=most_recent_iav2_graph_date,
            max_lookback=max_lookback_days,
        ).get()
    )
    logging.info(f'Using adbrain legacy graph date {most_recent_adbrain_graph_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(
        key=adbrain_graph_date_key,
        value=most_recent_adbrain_graph_date.strftime("%Y-%m-%d"),
    )

    most_recent_adbrain_open_graph_date = (
        ttd_graph.v2.default_client.dataset.check_recent_data_exist(
            cloud_storage=cloud_storage,
            ds_date=most_recent_iav2_graph_date,
            max_lookback=max_lookback_days,
        ).get()
    )
    logging.info(f'Using adbrain open graph date {most_recent_adbrain_open_graph_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(
        key=adbrain_open_graph_date_key,
        value=most_recent_adbrain_open_graph_date.strftime("%Y-%m-%d"),
    )

    most_recent_singleton_graph_date = (
        ttd_graph.v2.standard_input.singleton_persons.dataset.check_recent_data_exist(
            cloud_storage=cloud_storage,
            ds_date=most_recent_iav2_graph_date,
            max_lookback=max_lookback_days,
        ).get()
    )
    logging.info(f'Using v2 singleton graph date {most_recent_singleton_graph_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(
        key=singleton_graph_date_key,
        value=most_recent_singleton_graph_date.strftime("%Y-%m-%d"),
    )


calculate_dates_step = PythonOperator(
    task_id=calculate_dates_task_id,
    python_callable=calculate_dates,
    dag=airflow_dag,
    provide_context=True,
)

calculate_dates_task = OpTask(task_id="cal_graph_dates_task_id", op=calculate_dates_step)

regions = ["us-west-2", "ap-northeast-1", "ap-southeast-1", "eu-west-1", "eu-central-1"]

datasets = [Datasources.ctv.unified_open_graph]

# for each destination bucket, create a copy task
for region in regions:
    for dataset in datasets:
        copy_task = DatasetTransferTask(
            name=f"copy-{dataset.data_name}-to-{region}",
            dataset=dataset,
            src_cloud_provider=CloudProviders.aws,
            dst_cloud_provider=CloudProviders.aws,
            dst_region=region,
            partitioning_args=dataset.get_partitioning_args(ds_date=xcom_template_to_get_value(iav2_graph_date_key)),
        )
        dag >> calculate_dates_task >> cluster_task >> copy_task
