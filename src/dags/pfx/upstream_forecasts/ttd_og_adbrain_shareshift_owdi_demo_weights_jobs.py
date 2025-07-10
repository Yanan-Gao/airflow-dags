import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.tv.constants import FORECAST_JAR_PATH
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.emr_instance_types.compute_optimized.c5a import C5a
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

OWDI_DEMO_WEIGHT_JOB_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.DemoWeightsFromOwdiJobShareshift"
OWDI_MAX_COUNTS_JOB_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.DemoMaxCountsFromOwdiJob"

JAR_PATH = FORECAST_JAR_PATH

NUM_CORE_UNITS = 240

TTD_ENV = "prod"

owdi_date_key_1 = 'owdi_date_1'
owdi_date_key_2 = 'owdi_date_2'  # owdi_date_2 is latest, owdi_date_1 is roughly two weeks apart
owdi_demo_dag_id = "ttd-og-adbrain-shareshift-owdi-demo-weights-job-dag"
owdi_calculate_dates_task_id = "og-adbrain-shareshift-owdi-calculate-dates-task"

# dates used for down stream pipeline
target_date_key = "target-date"
demo_target_date_key = "demo-target-date"

job_start_date = datetime(2025, 5, 10)
job_end_date = None

owdi_ttd_dag = TtdDag(
    dag_id=owdi_demo_dag_id,
    start_date=job_start_date,
    end_date=job_end_date,
    schedule_interval=timedelta(days=7),
    slack_channel=PFX.team.alarm_channel,
    tags=["pfx", "demo", "owdi", "og"],
    max_active_runs=2,
)

owdi_demo_gen_dag = owdi_ttd_dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5d.m5d_4xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C5a.c5ad_2xlarge().with_fleet_weighted_capacity(8),
        C5a.c5ad_4xlarge().with_fleet_weighted_capacity(16),
    ],
    on_demand_weighted_capacity=NUM_CORE_UNITS,
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

cluster_task = EmrClusterTask(
    name="ttd-og-adbrain-shareshift-owdi-demo-weights-gen-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.PFX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2_1,
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations],
)


def xcom_template_to_get_value(key: str) -> str:
    global owdi_demo_dag_id, owdi_calculate_dates_task_id
    return f'{{{{ ' \
           f'task_instance.xcom_pull(dag_id="{owdi_demo_dag_id}", ' \
           f'task_ids="{owdi_calculate_dates_task_id}", ' \
           f'key="{key}") ' \
           f'}}}}'


additional_args_option_pairs_list = [("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

generate_owdi_demo_weights_emr_step_task = EmrJobTask(
    name="owdi-demo-weights-gen-step",
    class_name=OWDI_DEMO_WEIGHT_JOB_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("ttd.env", TTD_ENV),
        ("owdiDate1", xcom_template_to_get_value(owdi_date_key_1)),
        ("owdiDate2", xcom_template_to_get_value(owdi_date_key_2)),
        ("owdiPath", f"s3://ttd-ctv/shareshift/env={TTD_ENV}/geo-enriched-open-graph-owdi-person-demo/graph/"),
        ("s3RootPath", "s3://ttd-ctv/upstream-forecast/data-preparation-og-adbrain"),  # These two are additional for open graph version
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

cluster_task.add_sequential_body_task(generate_owdi_demo_weights_emr_step_task)


def calculate_owdi_dates(**context):
    dag_run = context.get('dag_run')
    if dag_run is not None:
        conf = dag_run.conf
    else:
        conf = None
    if conf is not None and "run-date" in conf:
        run_date_str = conf["run-date"]  # use run-date if defined in conf
    else:
        run_date_str = context['ts']  # "ts" is logical timestamp
    run_date = datetime.fromisoformat(run_date_str)
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    task_instance = context['task_instance']

    most_recent_owdi_date1 = Datasources.ctv.open_graph_adbrain_owdi_data.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=run_date - timedelta(days=14), max_lookback=60
    ).get()
    if most_recent_owdi_date1 is None:
        raise ValueError(f'did not find first owdi date based on date {run_date.strftime("%Y-%m-%d")}')
    logging.info(f'Using adbrain owdi date 1 {most_recent_owdi_date1.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=owdi_date_key_1, value=most_recent_owdi_date1.strftime("%Y-%m-%d"))

    most_recent_owdi_date2 = Datasources.ctv.open_graph_adbrain_owdi_data.check_recent_data_exist(
        cloud_storage=cloud_storage, ds_date=run_date, max_lookback=60
    ).get()
    if most_recent_owdi_date2 is None:
        raise ValueError(f'did not find second owdi date based on date {run_date.strftime("%Y-%m-%d")}')
    logging.info(f'Using adbrain owdi date 2 {most_recent_owdi_date2.strftime("%Y-%m-%d")}')
    task_instance.xcom_push(key=owdi_date_key_2, value=most_recent_owdi_date2.strftime("%Y-%m-%d"))

    # store keys for downstream fmap gen dag to use,
    # use run_date as fmap target-date and user profile second date as demo-target-date
    task_instance.xcom_push(key=target_date_key, value=run_date.strftime("%Y-%m-%d"))
    task_instance.xcom_push(key=demo_target_date_key, value=most_recent_owdi_date2.strftime("%Y-%m-%d"))


calculate_dates_step = PythonOperator(
    task_id=owdi_calculate_dates_task_id, python_callable=calculate_owdi_dates, dag=owdi_demo_gen_dag, provide_context=True
)

calculate_dates_task = OpTask(task_id='cal_avails_graph_owdi_dates_task_id', op=calculate_dates_step)

trigger_fmap_task = OpTask(
    op=TriggerDagRunOperator(
        dag=owdi_ttd_dag.airflow_dag,
        task_id='trigger-downstream-og-fmap-gen',
        trigger_dag_id="ttd-og-adbrain-shareshift-generate-fmap",
        conf={
            "target-date": xcom_template_to_get_value(target_date_key),
            "demo-target-date": xcom_template_to_get_value(demo_target_date_key),
            "look-back-days": "28",
        },
        wait_for_completion=True,
        poke_interval=300,  # 5 min
    )
)

owdi_ttd_dag >> calculate_dates_task >> cluster_task >> trigger_fmap_task
