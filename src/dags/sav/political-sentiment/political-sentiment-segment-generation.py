from datetime import datetime, timedelta
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.operators.python import PythonOperator
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import sav
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

PROVISIONING_CONN = 'provisioning_bi'  # provdb-bi.adsrvr.org
PROVISIONING_DB_NAME = 'Provisioning'
job_jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-sav-assembly.jar"
run_date = "{{ logical_date.strftime('%Y-%m-%d') }}"
dag_id = "political-sentiment-segment-generation"
get_data_provider_key_task_id = "get_data_provider_key_and_push_xcom"
data_provider_secret_name = "data_provider_secret"

job_start_date = datetime(2023, 11, 29)
job_schedule_interval = "0 7 * * *"

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    output_root = "s3://ttd-sav-pii-sandbox/env=test/political-sentiment"
else:
    output_root = "s3://ttd-sav-pii/env=prod/political-sentiment"


def get_data_provider_key_and_push_xcom(**context):
    sql_hook = MsSqlHook(mssql_conn_id=PROVISIONING_CONN, schema=PROVISIONING_DB_NAME)
    with sql_hook.get_conn() as conn, conn.cursor() as cursor:
        sql = """
                select SecretKey
                from ThirdPartyDataProvider
                where ThirdPartyDataProviderId = 'uspolitical';
               """
        cursor.execute(sql)
        row = cursor.fetchone()
        context['task_instance'].xcom_push(key=data_provider_secret_name, value=row[0])


dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    run_only_latest=True,
    slack_channel="#scrum-sav-alerts",
    tags=[sav.jira_team]
)

adag = dag.airflow_dag

get_data_provider_key_and_push_xcom = OpTask(
    task_id='cal_get_data_provider_key',
    op=PythonOperator(
        task_id=get_data_provider_key_task_id,
        provide_context=True,
        python_callable=get_data_provider_key_and_push_xcom,
        dag=adag,
    )
)

wait_for_avails_data = OpTask(
    op=DatasetCheckSensor(
        dag=adag,
        task_id='wait_for_avails_7_day',
        ds_date="{{ logical_date.at(23).to_datetime_string() }}",
        lookback=0,
        poke_interval=60 * 10,
        timeout=60 * 60 * 24,
        datasets=[AvailsDatasources.avails_7_day]
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
        R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
        R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
    ],
    on_demand_weighted_capacity=960,
)

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

cluster_task = EmrClusterTask(
    name="political-sentiment-segment-generation-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": sav.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    additional_application_configurations=[additional_application_configurations],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    log_uri=f"{output_root}/logs/airflow",
    enable_prometheus_monitoring=True,
)


def xcom_template_to_get_value(key: str) -> str:
    global dag_id, get_data_provider_key_task_id
    return (
        f"{{{{ "
        f'task_instance.xcom_pull(dag_id="{dag_id}", '
        f'task_ids="{get_data_provider_key_task_id}", '
        f'key="{key}") '
        f"}}}}"
    )


segment_generation_task = EmrJobTask(
    name="political-sentiment-segment-generation-task",
    class_name="jobs.sav.political.PoliticalSentimentSegmentGenerator",
    executable_path=job_jar,
    eldorado_config_option_pairs_list=[("AvailsDate", run_date), ("OutputPath", f"{output_root}/output/{run_date}")],
    additional_args_option_pairs_list=[("conf", "spark.sql.files.ignoreCorruptFiles=true")],
    configure_cluster_automatically=True,
    timeout_timedelta=timedelta(hours=4)
)

segment_export_task = EmrJobTask(
    name="political-sentiment-segment-export-task",
    class_name="jobs.sav.political.PoliticalSentimentSegmentExporter",
    executable_path=job_jar,
    eldorado_config_option_pairs_list=[("RunDate", run_date), ("InputPath", f"{output_root}/output"),
                                       ("DataProviderKey", xcom_template_to_get_value(data_provider_secret_name))],
    additional_args_option_pairs_list=[("conf", "spark.sql.files.ignoreCorruptFiles=true")],
    configure_cluster_automatically=True,
    timeout_timedelta=timedelta(hours=1)
)

segment_generation_task >> segment_export_task

cluster_task.add_parallel_body_task(segment_generation_task)
cluster_task.add_parallel_body_task(segment_export_task)

dag >> get_data_provider_key_and_push_xcom >> wait_for_avails_data >> cluster_task
