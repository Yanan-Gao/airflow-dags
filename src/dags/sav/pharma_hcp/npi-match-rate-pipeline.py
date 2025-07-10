from datetime import datetime, timedelta

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.kubernetes.secret import Secret

from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.slack.slack_groups import sav
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_id = "npi-match-rate-pipeline"
notification_slack_channel = "#scrum-sav-alerts"

npi_reporting_docker_image = "production.docker.adsrvr.org/ttd/npireporting:latest"
generator_task_trigger = 'Airflow'

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    profile = "staging"
    # no need to use k8s connection in prod test env as we've created the role binding for airflow2-service-account
    k8s_connection_id = None
    k8s_namespace = "npi-reporting-dev"
    service_account = "crm-data-service-account-staging"

    request_cpu = 1
    request_memory = 24
    limit_memory = 32
    request_ephemeral_storage = 16
    limit_ephemeral_storage = 64

else:
    profile = "prod"
    k8s_connection_id = 'npi-reporting-k8s'
    k8s_namespace = "npi-reporting"
    service_account = "crm-data-service-account-prod"

    request_cpu = 2
    request_memory = 16
    limit_memory = 32
    request_ephemeral_storage = 16
    limit_ephemeral_storage = 64

k8s_resources = PodResources.from_dict(
    request_cpu=f"{request_cpu}",
    request_memory=f"{request_memory}Gi",
    limit_memory=f"{limit_memory}Gi",
    request_ephemeral_storage=f"{request_ephemeral_storage}Gi",
    limit_ephemeral_storage=f"{limit_ephemeral_storage}Gi"
)

java_opts = (f"-Xms{request_memory}g "
             "-XX:+UseG1GC "
             "-XX:MaxGCPauseMillis=200 ")

secrets = [
    Secret(
        deploy_type="env",
        deploy_target="TTD_SPRING_DATASOURCE_PROVISIONING_PASSWORD",
        secret="npi-datasource",
        key="%s_PROVISIONING_PASSWORD" % profile.upper()
    ),
    Secret(
        deploy_type="env",
        deploy_target="TTD_SPRING_DATASOURCE_LOGWORKFLOW_PASSWORD",
        secret="npi-datasource",
        key="%s_LOGWORKFLOW_PASSWORD" % profile.upper()
    ),
    Secret(
        deploy_type="env", deploy_target="TTD_NPI_SWOOPAPI_CLIENTID", secret="npi-datasource", key="%s_SWOOPAPI_CLIENTID" % profile.upper()
    ),
    Secret(
        deploy_type="env",
        deploy_target="TTD_NPI_SWOOPAPI_CLIENTSECRET",
        secret="npi-datasource",
        key="%s_SWOOPAPI_CLIENTSECRET" % profile.upper()
    ),
    Secret(deploy_type="env", deploy_target="TTD_NPI_S3_SWOOP_ACCESSKEY", secret="npi-datasource", key="S3_SWOOP_ACCESSKEY"),
    Secret(deploy_type="env", deploy_target="TTD_NPI_S3_SWOOP_SECRETKEY", secret="npi-datasource", key="S3_SWOOP_SECRETKEY"),
]

job_schedule_interval = "0 0 * * *"
job_start_date = datetime(2025, 7, 4)

ttd_dag = TtdDag(
    dag_id=dag_id,
    slack_channel=notification_slack_channel,
    enable_slack_alert=True,
    start_date=job_start_date,
    run_only_latest=True,
    schedule_interval=job_schedule_interval,
    retries=3,
    max_active_runs=1,
    depends_on_past=True,
    retry_delay=timedelta(minutes=1),
    tags=[sav.jira_team],
)
dag = ttd_dag.airflow_dag

match_rate_envs = {
    "SPRING_PROFILES_ACTIVE": profile,
    "TTD_NPI_APPLICATION_GENERATORTASKTYPE": "NpiMatchRateGenerator",
    "TTD_NPI_APPLICATION_GENERATORTASKTRIGGER": generator_task_trigger,
    "JAVA_OPTS": java_opts
}

match_rate_generator_task = OpTask(
    op=TtdKubernetesPodOperator(
        kubernetes_conn_id=k8s_connection_id,
        namespace=k8s_namespace,
        service_account_name=service_account,
        image=npi_reporting_docker_image,
        image_pull_policy="Always",
        name="match_rate_generator",
        task_id="match_rate_generator",
        dnspolicy="ClusterFirst",
        get_logs=True,
        dag=dag,
        startup_timeout_seconds=600,
        execution_timeout=timedelta(hours=1),
        log_events_on_failure=True,
        resources=k8s_resources,
        secrets=secrets,
        env_vars=match_rate_envs
    )
)

ttd_dag >> match_rate_generator_task
