"""
Daily job to load Realytics brands into provisioning, and to send a file to Realytics via s3
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret

from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

# Configs
name = "ctv-realytics"
job_start_date = datetime(2024, 7, 16, 0, 0)
job_schedule_interval = timedelta(days=1)
latest_docker_image = 'production.docker.adsrvr.org/ttd/ctv/ispot-batch-processor:3.0.0'

ttd_dag = TtdDag(
    dag_id=name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["irr"]
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

brand_extractor = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='incremental-reach-reporting',
        image=latest_docker_image,
        name="realytics_brand_extractor",
        task_id="realytics_brand_extractor",
        dnspolicy='Default',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=500,
        service_account_name='incremental-reach-reporting',
        log_events_on_failure=True,
        secrets=[prov_db],
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': 'ctv-ispot-realytics-brand-extractor'
        },
        arguments=["RealyticsBrandExtractor"],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='1G', limit_memory='2G', request_cpu='1')
    )
)

ttd_dag >> brand_extractor
