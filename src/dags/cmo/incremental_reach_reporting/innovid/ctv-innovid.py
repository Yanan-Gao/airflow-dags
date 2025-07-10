"""
Daily job to load Innovid brands into Provisioning
"""

from airflow import DAG
from airflow.kubernetes.secret import Secret
from datetime import datetime, timedelta

from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

# Configs
name = "ctv-innovid"
job_start_date = datetime(2024, 7, 1, 0, 0)
job_schedule_interval = timedelta(days=30)
latest_docker_image = 'production.docker.adsrvr.org/ttd/ctv/ispot-batch-processor:3.0.0'

cluster_tags = {
    'Team': CMO.team.jira_team,
}

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
        name="innovid_brand_extractor",
        task_id="innovid_brand_extractor",
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
            'sumologic.com/sourceCategory': 'ctv-innovid-brand-extractor'
        },
        arguments=["InnovidBrandExtractor"],
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='500M', limit_memory='1G', request_cpu='1')
    )
)

ttd_dag >> brand_extractor
