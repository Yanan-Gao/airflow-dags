# from airflow import DAG
from datetime import datetime, timedelta
import logging

from airflow.operators.python_operator import ShortCircuitOperator

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import CHGROW
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.monads.trye import Try
from ttd.identity_graphs.identity_graphs import IdentityGraphs

identity_graphs = IdentityGraphs()
adbrain_opengraph_household_path = identity_graphs.ttd_graph.v2.standard_input.households_capped_for_hot_cache.dataset.get_dataset_path()

job_name = 'ctv-live-events-forecasting-demo-label-generation-opengraph'
cluster_name = 'ctv_live_events_forecasting_demo_label_generation_opengraph_cluster'
job_jar = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
job_class = 'com.thetradedesk.etlforecastjobs.preprocessing.demolabels.HHandPersonDeterministicDemoLabelGenerationOpenGraph'
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

job_start_date = datetime(2024, 12, 31, 2, 0)
job_schedule_interval = timedelta(days=1)
job_slack_channel = CHGROW.channels_growth().alarm_channel
active_running_jobs = 1

max_owdi_demolabels_dataset_lookback_days = 60
check_new_demo_label_data_task_id = "check-new-demo-label-data"
demo_label_data_date_key = "demo-label-date"

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6g_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6gd_4xlarge().with_fleet_weighted_capacity(16),
        M6g.m6gd_8xlarge().with_fleet_weighted_capacity(32),
        M6g.m6gd_12xlarge().with_fleet_weighted_capacity(48),
        M6g.m6gd_16xlarge().with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=384
)

live_events_demo_label_aggregation_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=CHGROW.channels_growth().sub_team,
    tags=["live events", "ctv", CHGROW.channels_growth().jira_team],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

live_events_demo_label_aggregation_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags={"Team": CHGROW.channels_growth().jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True
)

step = EmrJobTask(
    name=job_name,
    class_name=job_class,
    eldorado_config_option_pairs_list=[(
        "date",
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{check_new_demo_label_data_task_id}", key="{demo_label_data_date_key}") }}}}'
    ), ("householdOpenGraphAdBrainRootPath", adbrain_opengraph_household_path)],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4)
)
live_events_demo_label_aggregation_cluster.add_parallel_body_task(step)

dag = live_events_demo_label_aggregation_dag.airflow_dag


def check_new_demo_label_data(**context) -> bool:
    run_date_str = context['ds']
    run_date = datetime.strptime(run_date_str, '%Y-%m-%d')
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    demo_data_date_res: Try[datetime.date] = Datasources.datprd.owdi_demo_export_data.check_recent_data_exist(
        cloud_storage, run_date, max_owdi_demolabels_dataset_lookback_days
    )

    if demo_data_date_res.is_failure:
        msg = f'No demo data found in past {max_owdi_demolabels_dataset_lookback_days} days from {run_date_str}'
        print(msg)
        raise Exception(msg)

    demo_data_date = demo_data_date_res.get()
    demo_data_date_str = datetime.strftime(demo_data_date, '%Y-%m-%d')
    household_data_exists = Datasources.ctv.live_events_household_demo_labels_opengraph.as_write(
    ).check_data_exist(cloud_storage, demo_data_date)
    person_data_exists = Datasources.ctv.live_events_person_demo_labels_opengraph.as_write().check_data_exist(cloud_storage, demo_data_date)
    output_exists = household_data_exists and person_data_exists

    if output_exists:
        logging.info(f"Output already exists for date {demo_data_date_str}")
        return False

    logging.info(f"Output does not exist for date {demo_data_date_str}. Running the job.")

    task_instance = context['task_instance']
    task_instance.xcom_push(key=demo_label_data_date_key, value=demo_data_date_str)
    return True


check_new_demo_label_data_step = OpTask(
    op=ShortCircuitOperator(
        dag=dag, task_id=check_new_demo_label_data_task_id, python_callable=check_new_demo_label_data, provide_context=True
    )
)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

live_events_demo_label_aggregation_dag >> check_new_demo_label_data_step
check_new_demo_label_data_step >> live_events_demo_label_aggregation_cluster
live_events_demo_label_aggregation_cluster >> final_dag_check
