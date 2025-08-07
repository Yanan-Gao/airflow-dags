import json

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.ttdenv import TtdEnvFactory
from dags.aifun.general_slack_report.user_configs import (
    DEV_JOB_IMAGE, SlackReportConfig, WEEKLY_CONFIG, BIWEEKLY_CONFIG, MONTHLY_CONFIG, DAILY_CONFIG
)
from ttd.kubernetes.pod_resources import PodResources

env = TtdEnvFactory.get_from_system()
production_job_image = "production.docker.adsrvr.org/ttd-base/aifun/slack_team_report_sender:latest"
job_image = production_job_image if env.execution_env == "prod" else DEV_JOB_IMAGE


def json_dumps(d):
    return json.dumps(d)


def create_dag(config: SlackReportConfig):
    dag = TtdDag(
        dag_id=config.dag_name,
        start_date=config.start_date,
        schedule_interval=config.schedule,
        tags=["AIFUN"],
        enable_slack_alert=True,
    )
    adag = dag.airflow_dag
    adag.user_defined_filters = {"json_dumps": json_dumps}
    report_job = TtdKubernetesPodOperator(
        name="slack-report",
        random_name_suffix=True,
        namespace="slack-report-sender",
        image=job_image,
        task_id=config.job_name,
        get_logs=True,
        image_pull_policy="Always",
        startup_timeout_seconds=500,
        log_events_on_failure=True,
        service_account_name=f"slack-report-sender-{'prod' if env.execution_env == 'prod' else 'dev'}",
        annotations={
            "sumologic.com/include":
            "true",
            "sumologic.com/sourceCategory":
            "slack-report-sender",
            "sumologic.com/sourceName":
            f"slack-report-sender-{config.cadence.cadence_name}",
            "iam.amazonaws.com/role":
            f"arn:aws:iam::003576902480:role/service.slack-report-sender-"
            f"{'prod' if env.execution_env == 'prod' else 'dev'}",
            "eks.amazonaws.com/role-arn":
            f"arn:aws:iam::003576902480:role/service.slack-report-sender-"
            f"{'prod' if env.execution_env == 'prod' else 'dev'}",
        },
        secrets=config.secrets,
        cmds=["python3"],
        arguments=[
            "apps/main.py",
            "--env",
            "prod" if env.execution_env == "prod" else "dev",
            "--cadence",
            config.cadence.cadence_name,
            "--ds",
            "{{ data_interval_end | ds }}",
            "--airflow-config",
            "{{ (dag_run.conf | json_dumps) if dag_run else {} }}",
        ],
        resources=PodResources(limit_cpu="4", limit_memory="4G", request_cpu="0.5", request_memory="1G")
    )

    task = OpTask(op=report_job)
    dag >> task
    return dag.airflow_dag


daily_dag = create_dag(DAILY_CONFIG)
weekly_dag = create_dag(WEEKLY_CONFIG)
biweekly_dag = create_dag(BIWEEKLY_CONFIG)
monthly_dag = create_dag(MONTHLY_CONFIG)
