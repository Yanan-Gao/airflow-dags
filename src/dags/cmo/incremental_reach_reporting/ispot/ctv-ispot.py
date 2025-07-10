"""
Daily job to update brands within provisioning and generate iSpot Reach Reports
"""

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta

from datasources.sources.ispot_datasources import ISpotDatasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2024, 8, 6, 0, 0)
job_schedule_interval = timedelta(days=1)

pipeline_name = "ctv-ispot"
logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
latest_docker_image = 'production.docker.adsrvr.org/ttd/ctv/ispot-batch-processor:3.0.3'

# DAG
ttd_dag: TtdDag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["irr"],
    retries=1,
    retry_delay=timedelta(hours=1)
)
dag: DAG = ttd_dag.airflow_dag

####################################################################################################################
# Steps
####################################################################################################################

prov_db = Secret(
    deploy_type='env',
    deploy_target='ConnectionStrings__Provisioning',
    secret='incremental-reach-reporting-secrets',
    key='REACH_REPORTING_DB_CONNECTION'
)
client_id = Secret(deploy_type='env', deploy_target='iSpot__ClientId', secret='incremental-reach-reporting-secrets', key='ISPOT_CLIENT_ID')
client_secret = Secret(
    deploy_type='env', deploy_target='iSpot__ClientSecret', secret='incremental-reach-reporting-secrets', key='ISPOT_CLIENT_SECRET'
)
audience_client_id = Secret(
    deploy_type='env',
    deploy_target='iSpot__AudienceClientId',
    secret='incremental-reach-reporting-secrets',
    key='ISPOT_AUDIENCE_CLIENT_ID'
)
audience_client_secret = Secret(
    deploy_type='env',
    deploy_target='iSpot__AudienceClientSecret',
    secret='incremental-reach-reporting-secrets',
    key='ISPOT_AUDIENCE_CLIENT_SECRET'
)

brand_extractor = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='incremental-reach-reporting',
        image=latest_docker_image,
        name="brand_extractor",
        task_id="brand_extractor",
        dnspolicy='Default',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=500,
        service_account_name='incremental-reach-reporting',
        log_events_on_failure=True,
        secrets=[prov_db, client_id, client_secret, audience_client_id, audience_client_secret],
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': 'ctv-ispot-brand-extractor'
        },
        arguments=["BrandExtractor"],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='1G', limit_memory='2G', request_cpu='1')
    )
)

check_report_data = OpTask(
    op=DatasetRecencyOperator(
        dag=dag,
        datasets_input=[ISpotDatasources.ispot_reach_report],
        lookback_input=0,
        run_delta=timedelta(days=0),
        task_id="check_report_data",
        cloud_provider=CloudProviders.aws
    )
)


###########################################
#   Writes success file to S3
###########################################
def add_success_file(logical_date, **_):
    key = f"{ISpotDatasources.ispot_reach_report._get_full_key(date=logical_date)}/_SUCCESS"

    aws_storage = AwsCloudStorage(conn_id='aws_default')
    aws_storage.load_string('', key=key, bucket_name=ISpotDatasources.ispot_reach_report.bucket, replace=True)

    return f'Written key {key}'


add_ispot_success_file = OpTask(
    op=PythonOperator(task_id="add_ispot_success_file", provide_context=True, python_callable=add_success_file, dag=dag)
)

logworkflow_open_ispot_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if TtdEnvFactory.get_from_system() ==
            TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': 30001,  # dbo.fn_Enum_GatingType_DailyISpotReachReportProcessing()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'  # open gate for following day
            }
        },
        task_id="logworkflow_open_ispot_gate",
    )
)

# iSpot benchmark
first_day_of_last_month = "{{ (logical_date.replace(day=1) + macros.timedelta(days=-1)).replace(day=1).strftime(\"%Y-%m-%d\") }}"
last_day_of_last_month = "{{ (logical_date.replace(day=1) + macros.timedelta(days=-1)).strftime(\"%Y-%m-%d\") }}"

benchmark_generator = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='incremental-reach-reporting',
        image=latest_docker_image,
        name="benchmark_generator",
        task_id="benchmark_generator",
        dnspolicy='Default',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=500,
        log_events_on_failure=True,
        service_account_name='incremental-reach-reporting',
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': 'ctv-ispot-benchmark-generator'
        },
        secrets=[prov_db, client_id, client_secret],
        arguments=[
            "IrBenchmarkGenerator",
            "--start-date=" + first_day_of_last_month,
            "--end-date=" + last_day_of_last_month,  # last day of last month
            "--report-date=" + last_day_of_last_month  # the date showing up in the saved path
        ],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='1G', limit_memory='2G', request_cpu='1')
    )
)

is_day_to_run_benchmark_generator = OpTask(
    op=ShortCircuitOperator(
        dag=dag,
        task_id='should_run_benchmark_generator',
        python_callable=lambda logical_date, **_: logical_date.day == 4,
        provide_context=True
    )
)

# iSpot daily impression count
ispot_daily_impression_count = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='incremental-reach-reporting',
        image=latest_docker_image,
        name="ispot_number_of_daily_impressions",
        task_id="ispot_number_of_daily_impressions",
        dnspolicy='ClusterFirst',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=500,
        service_account_name='incremental-reach-reporting',
        log_events_on_failure=True,
        secrets=[client_id, client_secret],
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': 'ctv-ispot-daily-impressions-count'
        },
        arguments=["iSpotDailyImpressionCount"],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='500M', limit_memory='1G', request_cpu='1')
    )
)

ttd_dag >> brand_extractor
ttd_dag >> add_ispot_success_file >> check_report_data >> logworkflow_open_ispot_gate
ttd_dag >> is_day_to_run_benchmark_generator >> benchmark_generator
ttd_dag >> ispot_daily_impression_count
