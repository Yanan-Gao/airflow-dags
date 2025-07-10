"""
Daily job to send incremental and complete brand mapping CSVs to iSpot's S3 location.

This job was split out from the "ctv-ispot" DAG because the report-generator was taking longer than 24 hours,
which made the delivery of this file run at sporatic times. Once the report-generator is deprecated, these can be combined.
"""

from airflow import DAG
from airflow.kubernetes.secret import Secret
from datetime import datetime, timedelta

from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

job_start_date = datetime(2024, 8, 6, 0, 0)
job_schedule_interval = timedelta(days=1)

pipeline_name = "ctv-ispot-brand-mapping-delivery"
latest_docker_image = 'production.docker.adsrvr.org/ttd/ctv/ispot-batch-processor:3.0.0'

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

send_brand_mappings = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='incremental-reach-reporting',
        image=latest_docker_image,
        name="get_brand_mappings",
        task_id="get_brand_mappings",
        dnspolicy='Default',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=500,
        log_events_on_failure=True,
        service_account_name='incremental-reach-reporting',
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': 'ctv-ispot-brand-mapping-delivery'
        },
        secrets=[prov_db, client_id, client_secret],
        arguments=["GetBrandMappings", "--report-date={{logical_date.strftime(\"%Y-%m-%d\")}}"],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='500M', limit_memory='1G', request_cpu='1')
    )
)
ttd_dag >> send_brand_mappings
