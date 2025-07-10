from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.pfx.upstream_forecasts import upstream_utils
from dags.tv.constants import FORECAST_JAR_PATH
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from datetime import datetime

from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from datasources.datasources import Datasources
import logging

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import PFX
from datetime import timedelta
from typing import Tuple, List, Any, Optional, Sequence

from ttd.tasks.op import OpTask

dag_id = "ctv-legacy-demo-weights"
job_start_date = datetime(2024, 10, 29)  # starting Tuesday
job_end_date = None  # will retire this legacy dag eventually

demo_ttd_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    end_date=job_end_date,
    schedule_interval=timedelta(days=7),
    max_active_runs=1,
    tags=["pfx", "upstreamforecast", "demo weights"],
)

demo_airflow_dag = demo_ttd_dag.airflow_dag

prometheus_monitoring = True
calculate_dates_task_id = "calculate_dates"
graph_date_key = "graph_date"
user_profile_1_date_key = "user_profile_1"
user_profile_2_date_key = "user_profile_2"

# dates used for down stream pipeline
target_date_key = "target-date"
demo_target_date_key = "demo-target-date"

# Allows easy testing that the DAG is correct (clusters are spun up with a single
# machine and the Spark jobs exit immediately when the "-DdryRun" parameter is
# passed to them.
is_dry_run = False


def create_demo_cluster(name: str, num_4xls: int):
    master_config = [M5.m5_4xlarge().with_fleet_weighted_capacity(1)]
    instance_config = [
        M5.m5_4xlarge().with_fleet_weighted_capacity(1),
        M5.m5_8xlarge().with_fleet_weighted_capacity(2),
        M5.m5_16xlarge().with_fleet_weighted_capacity(4),
    ]

    return EmrClusterTask(
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        name=name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=master_config,
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_config, on_demand_weighted_capacity=num_4xls),
        enable_prometheus_monitoring=prometheus_monitoring,
        cluster_auto_terminates=True,
        cluster_tags={"Team": PFX.team.jira_team},
    )


def create_demo_cluster_task(
    class_name: str,
    timeout_timedelta: timedelta,
    job_params: List[Tuple[str, Any]],
    num_4xl_machines: int,
    additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
    job_instance_suffix_id: str = None,
) -> EmrClusterTask:
    global is_dry_run
    step_name = (f"{class_name}-{job_instance_suffix_id}" if job_instance_suffix_id else class_name)
    if is_dry_run:
        num_4xl_machines = 1
        job_params.append(("dryRun", "true"))

    cluster = create_demo_cluster(step_name, num_4xl_machines)

    step = EmrJobTask(
        cluster_specs=cluster.cluster_specs,
        name=step_name,
        class_name=f"com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.{class_name}",
        executable_path=FORECAST_JAR_PATH,
        timeout_timedelta=timeout_timedelta,
        eldorado_config_option_pairs_list=job_params,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )
    cluster.add_sequential_body_task(step)
    return cluster


filter_user_profiles_step_addl_spark_args = [
    # The BloomFilter data that we collect and broadcast to all executors is ~67Mb
    # By default the kryo serializer buffer max is 64Mb. To make collecting and broadcasting
    # possible we need to increase this size
    # Exception seen during testing:
    # org.apache.spark.SparkException: Kryo serialization failed: Buffer overflow. Available: 0,
    #   required: 67108864. To avoid this, increase spark.kryoserializer.buffer.max value
    ("conf", "spark.kryoserializer.buffer.max=80m")
]


def xcom_template_to_get_value(key: str) -> str:
    global dag_id, calculate_dates_task_id
    return (f"{{{{ "
            f'task_instance.xcom_pull(dag_id="{dag_id}", '
            f'task_ids="{calculate_dates_task_id}", '
            f'key="{key}") '
            f"}}}}")


demo_weights_filter_user_profiles_households_date1 = create_demo_cluster_task(
    class_name="DemoWeightsFilterUserProfilesHouseholdsJob",
    timeout_timedelta=timedelta(hours=2),
    num_4xl_machines=12,
    job_params=[
        ("availsFilterDate", xcom_template_to_get_value(graph_date_key)),
        ("userProfilesDate", xcom_template_to_get_value(user_profile_1_date_key)),
    ],
    additional_args_option_pairs_list=filter_user_profiles_step_addl_spark_args,
    job_instance_suffix_id="date1",
)

demo_weights_filter_user_profiles_households_date2 = create_demo_cluster_task(
    class_name="DemoWeightsFilterUserProfilesHouseholdsJob",
    timeout_timedelta=timedelta(hours=2),
    num_4xl_machines=12,
    job_params=[
        ("availsFilterDate", xcom_template_to_get_value(graph_date_key)),
        ("userProfilesDate", xcom_template_to_get_value(user_profile_2_date_key)),
    ],
    additional_args_option_pairs_list=filter_user_profiles_step_addl_spark_args,
    job_instance_suffix_id="date2",
)

demo_weights_union = create_demo_cluster_task(
    class_name="DemoWeightsUnionJob",
    timeout_timedelta=timedelta(minutes=30),
    num_4xl_machines=5,
    job_params=[
        ("date1", xcom_template_to_get_value(user_profile_1_date_key)),
        ("date2", xcom_template_to_get_value(user_profile_2_date_key)),
    ],
)

demo_weights_graph_filter = create_demo_cluster_task(
    class_name="DemoWeightsGraphFilterJob",
    timeout_timedelta=timedelta(minutes=60),
    num_4xl_machines=12,
    job_params=[("date", xcom_template_to_get_value(graph_date_key))],
)

demo_weights_graph_join_and_hh_demos = create_demo_cluster_task(
    class_name="DemoWeightsGraphJoinAndHHDemosJob",
    timeout_timedelta=timedelta(minutes=30),
    num_4xl_machines=3,
    job_params=[
        ("graphDate", xcom_template_to_get_value(graph_date_key)),
        ("userProfilesDate", xcom_template_to_get_value(user_profile_2_date_key)),
    ],
)


def calculate_dates(**context):
    max_lookback_days = 21
    run_date_str = context["ds"]
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d")
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    # Check the latest graph (geo enriched version which is needed by shareshift)
    most_recent_graph_date = (
        Datasources.ctv.upstream_forecast_geo_enriched_graph_data.check_recent_data_exist(
            cloud_storage=cloud_storage,
            ds_date=run_date,
            max_lookback=max_lookback_days,
        ).get()
    )

    # Check last 2 dates under s3a://thetradedesk-useast-hadoop/unified-data-models/prod/user-profiles/avails
    (most_recent_user_profile_date, second_most_recent_user_profile_date) = (upstream_utils.calculate_dates(run_date))

    logging.info(f'Using graph date {most_recent_graph_date.strftime("%Y-%m-%d")}')
    logging.info(f'Using user prof 1 date {most_recent_user_profile_date.strftime("%Y-%m-%d")}')
    logging.info(f'Using user prof 2 date {second_most_recent_user_profile_date.strftime("%Y-%m-%d")}')

    task_instance = context["task_instance"]
    task_instance.xcom_push(key=graph_date_key, value=most_recent_graph_date.strftime("%Y-%m-%d"))
    task_instance.xcom_push(
        key=user_profile_1_date_key,
        value=second_most_recent_user_profile_date.strftime("%Y-%m-%d"),
    )
    task_instance.xcom_push(
        key=user_profile_2_date_key,
        value=most_recent_user_profile_date.strftime("%Y-%m-%d"),
    )

    # store keys for downstream fmap gen dag to use,
    # use run_date as fmap target-date and user profile second date as demo-target-date
    task_instance.xcom_push(key=target_date_key, value=run_date.strftime("%Y-%m-%d"))
    task_instance.xcom_push(
        key=demo_target_date_key,
        value=most_recent_user_profile_date.strftime("%Y-%m-%d"),
    )


calculate_dates_step = OpTask(
    op=PythonOperator(
        task_id=calculate_dates_task_id,
        python_callable=calculate_dates,
        dag=demo_airflow_dag,
        provide_context=True,
    )
)


# Define the BranchOperator to call TriggerDagRunOperator conditionally based on conf flag trigger_downstream_dag
def create_dag_run(context, dag_run_obj):
    task_instance = context["task_instance"]
    print(str(task_instance))
    target_date = task_instance.xcom_pull(dag_id=dag_id, task_ids=calculate_dates_task_id, key=target_date_key)
    demo_date = task_instance.xcom_pull(dag_id=dag_id, task_ids=calculate_dates_task_id, key=demo_target_date_key)
    print(f"target-date:{target_date} and demo-date:{demo_date}")
    dag_run_obj.payload = {"target-date": target_date, "demo-target-date": demo_date}
    return dag_run_obj


trigger_fmap_gen_task = OpTask(
    op=TriggerDagRunOperator(
        task_id="trigger_fmap_gen_task",
        trigger_dag_id="ctv-legacy-generate-fmap",
        dag=demo_airflow_dag,
        conf={
            "target-date": xcom_template_to_get_value(target_date_key),
            "demo-target-date": xcom_template_to_get_value(demo_target_date_key),
            "look-back-days": "28",
        },
        wait_for_completion=True,
        poke_interval=300,  # 5 min
    )
)


def should_trigger_downstream_fmap_gen(**context):
    # if it is auto scheduled, we trigger downstream
    is_scheduled = not context.get("dag_run").external_trigger  # type: ignore
    logging.info(f"is auto scheduled:{str(is_scheduled)}")
    if is_scheduled:
        return "trigger_fmap_gen_task"

    # manually scheduled with conf flag enabled, we trigger downstream
    conf_value = context["dag_run"].conf.get("trigger-downstream-dag")
    if conf_value == "True":
        return "trigger_fmap_gen_task"
    else:
        return "do_nothing_task"


branch_op = OpTask(
    op=BranchPythonOperator(
        task_id="branch_task",
        provide_context=True,
        python_callable=should_trigger_downstream_fmap_gen,
        dag=demo_airflow_dag,
    )
)

do_nothing_task = OpTask(op=EmptyOperator(task_id="do_nothing_task", dag=demo_airflow_dag))

demo_ttd_dag >> calculate_dates_step >> demo_weights_graph_filter

demo_weights_graph_filter >> demo_weights_filter_user_profiles_households_date1 >> demo_weights_union
demo_weights_graph_filter >> demo_weights_filter_user_profiles_households_date2 >> demo_weights_union

demo_weights_union >> demo_weights_graph_join_and_hh_demos >> branch_op
branch_op >> trigger_fmap_gen_task
branch_op >> do_nothing_task
