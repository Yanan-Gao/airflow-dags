from datetime import datetime, timedelta

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.kubernetes.secret import Secret

from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.slack.slack_groups import sav
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_id = "npi-pulling-pipeline"
notification_slack_channel = "#scrum-sav-alerts"

npi_reporting_docker_image = "production.docker.adsrvr.org/ttd/npireporting:latest"
generator_task_trigger = 'Airflow'

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    profile = "staging"
    k8s_connection_id = None
    k8s_namespace = "npi-reporting-dev"
    service_account = "crm-data-service-account-staging"
    k8s_resources = PodResources.from_dict(
        request_cpu="1", request_memory="2Gi", limit_memory="4Gi", request_ephemeral_storage="16Gi", limit_ephemeral_storage="32Gi"
    )
    spark_env_path = "test"
else:
    profile = "prod"
    k8s_connection_id = 'npi-reporting-k8s'
    k8s_namespace = "npi-reporting"
    service_account = "crm-data-service-account-prod"
    k8s_resources = PodResources.from_dict(
        request_cpu="1", request_memory="8Gi", limit_memory="16Gi", request_ephemeral_storage="16Gi", limit_ephemeral_storage="32Gi"
    )
    spark_env_path = "prod"

secrets = [
    Secret(
        deploy_type="env",
        deploy_target="TTD_SPRING_DATASOURCE_PROVISIONING_PASSWORD",
        secret="npi-datasource",
        key="%s_PROVISIONING_PASSWORD" % profile.upper()
    ),
    Secret(
        deploy_type="env",
        deploy_target="TTD_NPI_PULLING_SFTPCONFIGJSON",
        secret="npi-datasource",
        key="%s_TTD_PULLING_SFTP_TOKEN" % profile.upper()
    ),
    Secret(
        deploy_type="env",
        deploy_target="TTD_NPI_TTDAPI_SERVICEACCOUNTTOKEN",
        secret="npi-datasource",
        key="%s_TTD_API_TOKEN" % profile.upper()
    ),
]

job_schedule_interval = "0 * * * *"
job_start_date = datetime(2025, 5, 20)

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

npi_pulling_task = OpTask(
    op=TtdKubernetesPodOperator(
        kubernetes_conn_id=k8s_connection_id,
        namespace=k8s_namespace,
        service_account_name=service_account,
        image=npi_reporting_docker_image,
        image_pull_policy="Always",
        name="pharma_hcp_npi_pulling",
        task_id="pharma_hcp_npi_pulling",
        dnspolicy="ClusterFirst",
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=600,
        execution_timeout=timedelta(hours=1),
        log_events_on_failure=True,
        resources=k8s_resources,
        secrets=secrets,
        env_vars={
            "SPRING_PROFILES_ACTIVE": profile,
            "TTD_NPI_APPLICATION_GENERATORTASKTYPE": "NPIPullingJob",
            "TTD_NPI_APPLICATION_GENERATORTASKTRIGGER": generator_task_trigger
        }
    )
)

ttd_dag >> npi_pulling_task
