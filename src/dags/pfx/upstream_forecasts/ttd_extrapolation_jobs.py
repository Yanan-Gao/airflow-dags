from functools import partial

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator

from dags.pfx.constants import UPSTREAM_FORECAST_ROOT
from dags.pfx.upstream_forecasts.ttd_generate_reach_curve_pyspark import base_job_name, ReachCurveScheduling
from dags.tv.constants import FORECAST_JAR_PATH
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.cloud_provider import CloudProviders
from datetime import datetime, timedelta, date
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from datasources.datasources import Datasources
import logging

from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

AVAILS_JOIN_GRAPH_AND_DEMO_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.extrapolation.AvailsJoinGraphAndDemoJob"
EXTRAPOLATION_GENERATOR_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.extrapolation.ExtrapolationGeneratorJob"
EXTRAPOLATION_ADJUST_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.extrapolation.ExtrapolationAdjustmentJob"
FMAP_EXTRAPOLATION_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.extrapolation.FMapExtrapolationJob"

JAR_PATH = FORECAST_JAR_PATH

iav2_graph_date_key = 'iav2_graph_date'
owdi_date_key_1 = 'owdi_date_1'
owdi_date_key_2 = 'owdi_date_2'  # owdi_date_2 is latest, owdi_date_1 is roughly two weeks apart
avails_date_key = 'avails_date'
fmap_date_key = 'fmap_date'
fmap_date_no_dashes_key = 'fmap_date_no_dashes'
calculate_graph_and_demo_dates_task_id = 'calculate_graph_and_demo_dates_task_id'
# job_schedule_interval = timedelta(days=7)
job_start_date = datetime(2024, 7, 1)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_4xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_8xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_4xlarge().with_fleet_weighted_capacity(2),
                    R5d.r5d_8xlarge().with_fleet_weighted_capacity(4)],
    on_demand_weighted_capacity=160
)


def xcom_template_to_get_value(
    key: str,
    dag_id: str,
) -> str:
    return f'{{{{ ' \
           f'task_instance.xcom_pull(dag_id="{dag_id}", ' \
           f'task_ids="{calculate_graph_and_demo_dates_task_id}", ' \
           f'key="{key}") ' \
           f'}}}}'


def create_extrapolation_dag(dag_id: str, graph_type: str, fmap_version: str) -> tuple[TtdDag, BaseOperator | None]:
    dag = TtdDag(
        dag_id=dag_id,
        start_date=job_start_date,
        # schedule_interval=job_schedule_interval,
        slack_channel=PFX.team.alarm_channel,
        tags=["ctv", "extrapolation", "reach curve"],
        max_active_runs=1,
    )

    cluster_name = "ctv-reach-curve-extrapolation-cluster"
    additional_application_configurations = {
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        },
    }
    s3_root_path = f"{UPSTREAM_FORECAST_ROOT}/data-preparation-{fmap_version}"

    cluster_task = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        cluster_tags={"Team": slack_groups.PFX.team.jira_team},
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label="emr-6.7.0",
        enable_prometheus_monitoring=True,
        additional_application_configurations=[additional_application_configurations],
    )

    additional_args_option_pairs_list = [("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

    generate_avails_join_demo_and_graph_task = EmrJobTask(
        name="gen-avails-join-graph-and-demo",
        class_name=AVAILS_JOIN_GRAPH_AND_DEMO_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[("ttd.env", "prod"), ("iav2Date", xcom_template_to_get_value(iav2_graph_date_key, dag_id)),
                                           ("owdiDate1", xcom_template_to_get_value(owdi_date_key_1, dag_id)),
                                           ("owdiDate2", xcom_template_to_get_value(owdi_date_key_2, dag_id)),
                                           ("availsDate", xcom_template_to_get_value(avails_date_key, dag_id)), ("availsLookBack", 28),
                                           ("graphType", graph_type), ("s3RootPath", s3_root_path)],
        cluster_specs=cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )

    generate_init_extrapolation_task = EmrJobTask(
        name="gen-int-extrapolation",
        class_name=EXTRAPOLATION_GENERATOR_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[("ttd.env", "prod"), ("availsDate", xcom_template_to_get_value(avails_date_key, dag_id)),
                                           ("s3RootPath", s3_root_path)],
        cluster_specs=cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )

    generate_adjusted_extrapolation_task = EmrJobTask(
        name="gen-adjusted-extrapolation",
        class_name=EXTRAPOLATION_ADJUST_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[("ttd.env", "prod"), ("extrapolationDate", xcom_template_to_get_value(avails_date_key, dag_id)),
                                           ("fmapDate", xcom_template_to_get_value(fmap_date_key, dag_id)), ("s3RootPath", s3_root_path)],
        cluster_specs=cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )

    generate_extrapolated_fmap_task = EmrJobTask(
        name="gen-extrapolated-fmap",
        class_name=FMAP_EXTRAPOLATION_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[("ttd.env", "prod"), ("extrapolationDate", xcom_template_to_get_value(avails_date_key, dag_id)),
                                           ("fmapDate", xcom_template_to_get_value(fmap_date_key, dag_id)), ("s3RootPath", s3_root_path)],
        cluster_specs=cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )

    cluster_task.add_sequential_body_task(generate_avails_join_demo_and_graph_task)
    cluster_task.add_sequential_body_task(generate_init_extrapolation_task)
    cluster_task.add_sequential_body_task(generate_adjusted_extrapolation_task)
    cluster_task.add_sequential_body_task(generate_extrapolated_fmap_task)

    calculate_dates_step = PythonOperator(
        task_id=calculate_graph_and_demo_dates_task_id,
        python_callable=partial(calculate_graph_demo_dates, fmap_version=fmap_version),
        dag=dag.airflow_dag,
        provide_context=True
    )

    calculate_dates_task = OpTask(task_id='cal_avails_graph_owdi_dates_task_id', op=calculate_dates_step)
    dag >> calculate_dates_task >> cluster_task
    return dag, cluster_task.last_airflow_op()


def calculate_graph_demo_dates(fmap_version: str, **context):
    max_lookback_days = 21
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    task_instance = context['task_instance']

    # need now() to make sure we have enough user sampled avails for 28 days
    most_recent_avails_date = AvailsDatasources.avails_30_day.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=date.today(), max_lookback=3
    ).get() - timedelta(days=1)
    logging.info(f'Using user sampled avails ending date {most_recent_avails_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=avails_date_key, value=most_recent_avails_date.strftime("%Y-%m-%d"))
    # make sure we have 28 days of avails, otherwise fail the job right away
    oldest_avails_date = AvailsDatasources.avails_30_day.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=most_recent_avails_date - timedelta(28), max_lookback=0
    ).get()
    if oldest_avails_date is None:
        raise ValueError("Not enough 28 days tracking back for the above date!")
    else:
        logging.info(f'Using user sampled avails starting date {oldest_avails_date.strftime("%Y-%m-%d")}')
    most_recent_iav2_graph_date = Datasources.ctv.iav2_legacy_graph.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=most_recent_avails_date, max_lookback=max_lookback_days
    ).get()
    logging.info(f'Using iav2 graph date {most_recent_iav2_graph_date.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=iav2_graph_date_key, value=most_recent_iav2_graph_date.strftime("%Y-%m-%d"))

    most_recent_owdi_date1 = Datasources.ctv.owdi_data.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=most_recent_avails_date - timedelta(days=14), max_lookback=14
    ).get()
    logging.info(f'Using owdi date 1 {most_recent_owdi_date1.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=owdi_date_key_1, value=most_recent_owdi_date1.strftime("%Y-%m-%d"))

    most_recent_owdi_date2 = Datasources.ctv.owdi_data.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=most_recent_avails_date, max_lookback=14
    ).get()
    logging.info(f'Using owdi date 2 {most_recent_owdi_date2.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=owdi_date_key_2, value=most_recent_owdi_date2.strftime("%Y-%m-%d"))

    # use fmap specified by fmap_version increase 3 days to pick up fmap latest
    most_recent_fmap_date = Datasources.ctv.get_upstream_forecast_fmap(fmap_version).check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=most_recent_avails_date + timedelta(days=3), max_lookback=15
    ).get()
    logging.info(f'Using fmap date {most_recent_fmap_date.strftime("%Y-%m-%d")}')

    task_instance.xcom_push(key=fmap_date_key, value=most_recent_fmap_date.strftime("%Y-%m-%d"))


# Legacy IAV2 Graph DAG
legacy_extrapolation_dag, legacy_extrapolation_last_op = create_extrapolation_dag(
    dag_id='ttd-reach-curve-extrapolation-dag', graph_type='iav2', fmap_version="v3"
)
legacy_extrapolation_adag = legacy_extrapolation_dag.airflow_dag

trigger_reach_curve_gen_v3_extrapolation = ReachCurveScheduling.create_trigger_reach_curve_gen_dag_task(
    parent_dag=legacy_extrapolation_adag,
    trigger_dag_id=f"{base_job_name}-v3-extrapolated",
    trigger_input_date=xcom_template_to_get_value(fmap_date_key, legacy_extrapolation_adag.dag_id)
)

legacy_extrapolation_last_op >> trigger_reach_curve_gen_v3_extrapolation

# OpenGraph IAV2 Graph DAG
og_extrapolation_dag, og_extrapolation_last_op = create_extrapolation_dag(
    dag_id='ttd-og-reach-curve-extrapolation-dag', graph_type='openGraphIav2', fmap_version="og"
)
og_extrapolation_adag = og_extrapolation_dag.airflow_dag

trigger_reach_curve_gen_og = ReachCurveScheduling.create_trigger_reach_curve_gen_dag_task(
    parent_dag=og_extrapolation_adag,
    trigger_dag_id=f"{base_job_name}-og",
    trigger_input_date=xcom_template_to_get_value(fmap_date_key, og_extrapolation_adag.dag_id)
)

og_extrapolation_last_op >> trigger_reach_curve_gen_og
