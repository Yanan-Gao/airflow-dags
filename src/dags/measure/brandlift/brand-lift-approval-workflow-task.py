from datetime import datetime, timedelta

from ttd.eldorado.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

from ttd.ttdenv import TtdEnvFactory

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    alarms_slack_channel = "#scrum-measurement-up-alarms"
    vault_k8s_conn_id = "brand-lift-approval-workflow-connection-prod"
    k8s_namespace = "ttd-brandliftapi"
    approval_task_docker_image = "docker.pkgs.adsrvr.org/apps-prod/brandliftapprovalworkflowtask:1.0.14"
    jira_cloud_base_url = "https://thetradedesk.atlassian.net"
else:
    alarms_slack_channel = "#test-ftr-political-brand-lift-notify"
    vault_k8s_conn_id = "brand-lift-approval-workflow-connection-dev"
    k8s_namespace = "ttd-brandliftapi-dev"
    approval_task_docker_image = "internal.docker.adsrvr.org/brandliftapprovalworkflowtask:1.0.8-lha-measure-6749-jira-approval.g591e68a3ca"
    jira_cloud_base_url = "https://thetradedesk-sandbox.atlassian.net"

brand_lift_approval_workflow_dag = TtdDag(
    dag_id="brand-lift-approval-workflow-task",
    start_date=datetime(2025, 3, 20),
    schedule_interval="0 * * * *",
    max_active_runs=1,
    retries=2,
    depends_on_past=False,
    tags=["measurement", "brand-lift-approval-task"],
    slack_channel=alarms_slack_channel,
    run_only_latest=True,
)

dag = brand_lift_approval_workflow_dag.airflow_dag

brand_lift_approval_workflow_task = TtdKubernetesPodOperator(
    namespace=k8s_namespace,
    kubernetes_conn_id=vault_k8s_conn_id,
    image=approval_task_docker_image,
    image_pull_policy="Always",
    name="brandlift-approval-workflow-task",
    task_id="brandlift-approval-workflow-task",
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
    startup_timeout_seconds=500,
    execution_timeout=timedelta(hours=1),
    log_events_on_failure=True,
    service_account_name="brandlift",
    annotations={
        "sumologic.com/include": "true",
        "sumologic.com/sourceCategory": "brandlift-approval-workflow-task",
    },
    env_vars={
        "TTD_ApprovalJiraCloud__BaseUrl": jira_cloud_base_url,
        "TTD_AppTelemetry__Metrics__PrometheusGateway": "http://prometheus-pushgateway.monitoring.svc.cluster.local:9091/metrics",
        "TTD_AppTelemetry__Metrics__PrometheousJob": "brand-lift-approval-workflow-task"
    },
    secrets=[
        Secret(
            deploy_type="env",
            deploy_target="TTD_ApprovalJiraCloud__Username",
            secret="brandlift-approval-jiracloud-policy",
            key="username",
        ),
        Secret(
            deploy_type="env",
            deploy_target="TTD_ApprovalJiraCloud__Password",
            secret="brandlift-approval-jiracloud-policy",
            key="password",
        ),
        Secret(
            deploy_type="env",
            deploy_target="TTD_ServiceConfig__BrandLiftConnectionString",
            secret="brandlift-db",
            key="ConnectionString",
        )
    ],
    resources=PodResources(
        request_cpu="500m",
        limit_cpu="1000m",
        request_memory="1Gi",
        limit_memory="2Gi",
        limit_ephemeral_storage="500Mi",
    )
)

brand_lift_approval_workflow_dag
