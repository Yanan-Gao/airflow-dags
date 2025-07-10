# from airflow import DAG
from datetime import datetime, timedelta
import logging

from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExecuteOnDemandDataMove
from ttd.tasks.op import OpTask
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import CHGROW
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.monads.trye import Try
from ttd.ttdenv import TtdEnvFactory

job_name = 'content-library-gracenote-title-taxonomy'
cluster_name = 'content-library-gracenote-title-taxonomy-cluster'
job_jar = 's3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/channels/chgrow/channelsgrowthspark-assembly.jar'
job_class = 'com.thetradedesk.channels.chgrow.channelsgrowthspark.pipelines.contentlibrary.GenerateTitleTaxonomy'
log_uri = 's3://ttd-identity/datapipeline/logs/airflow'
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

job_start_date = datetime(2024, 12, 6, 10, 0)
job_schedule_interval = timedelta(days=1)
job_slack_channel = CHGROW.channels_growth().alarm_channel
active_running_jobs = 1

max_gracenote_dataset_lookback_days = 14
check_new_gracenote_data_task_id = "check-new-gracenote-data"
gracenote_data_date_key = "gracenote-data-date"

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

content_library_gracenote_title_taxonomy_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=CHGROW.channels_growth().sub_team,
    tags=["content library", "gracenote", CHGROW.channels_growth().jira_team],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

gracenote_title_taxonomy_cluster = EmrClusterTask(
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
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{check_new_gracenote_data_task_id}", key="{gracenote_data_date_key}") }}}}'
    )],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4)
)
gracenote_title_taxonomy_cluster.add_parallel_body_task(step)

dag = content_library_gracenote_title_taxonomy_dag.airflow_dag


def check_new_gracenote_data(**context) -> bool:
    run_date_str = context['ds']
    run_date = datetime.strptime(run_date_str, '%Y-%m-%d')
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    gracenote_data_date_res: Try[datetime.date] = Datasources.contentlibrary.gracenote_title_taxonomy_external_data.check_recent_data_exist(
        cloud_storage, run_date, max_gracenote_dataset_lookback_days
    )

    if gracenote_data_date_res.is_failure:
        msg = f'No gracenote external taxonomy data found in past {max_gracenote_dataset_lookback_days} days from {run_date_str}'
        print(msg)
        raise Exception(msg)

    gracenote_data_date = gracenote_data_date_res.get()
    gracenote_data_date_str = datetime.strftime(gracenote_data_date, '%Y-%m-%d')
    gracenote_title_taxonomy_data_exists = Datasources.contentlibrary.gracenote_title_taxonomy_data.as_write() \
        .check_data_exist(cloud_storage, gracenote_data_date)

    if gracenote_title_taxonomy_data_exists:
        logging.info(f"Output already exists for date {gracenote_data_date_str}")
        return False

    logging.info(f"Output does not exist for date {gracenote_data_date_str}. Running the job.")

    task_instance = context['task_instance']
    task_instance.xcom_push(key=gracenote_data_date_key, value=gracenote_data_date_str)
    return True


check_new_gracenote_data_step = OpTask(
    op=ShortCircuitOperator(
        dag=dag, task_id=check_new_gracenote_data_task_id, python_callable=check_new_gracenote_data, provide_context=True
    )
)

execute_on_demand_datamove_gracenote_content_dataset = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExecuteOnDemandDataMove,
        op_kwargs={
            'mssql_conn_id': 'lwdb' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'sandbox-lwdb',
            'sproc_arguments': {
                'taskId':
                1000637,  # dbo.fn_Enum_Task_OnDemandImportGracenoteContent()
                'prefix':
                f'date={{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{check_new_gracenote_data_task_id}", key="{gracenote_data_date_key}") }}}}/'
            }
        },
        task_id="execute_on_demand_datamove_gracenote_content_dataset"
    )
)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

content_library_gracenote_title_taxonomy_dag >> check_new_gracenote_data_step
check_new_gracenote_data_step >> gracenote_title_taxonomy_cluster

gracenote_title_taxonomy_cluster >> execute_on_demand_datamove_gracenote_content_dataset
execute_on_demand_datamove_gracenote_content_dataset >> final_dag_check
