import os
from datetime import timedelta, datetime
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.workers.worker import Workers

from dags.aifun.lineage.constants import get_aws_arn, get_service_account, ingestion_task_env_str
from ttd.ttdenv import TtdEnvFactory

lineage_image = "production.docker.adsrvr.org/ttd-base/mlops/amundsen-catalog:latest"
job_name = "dataset-migration-monitoring"

job_start_date = datetime(2025, 3, 16, 18, 0)
env = TtdEnvFactory.get_from_system()

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=timedelta(days=1),
    slack_tags="AIFUN",
    enable_slack_alert=True,
    max_active_runs=1,
)
adag = dag.airflow_dag
lineage_spec_path = os.path.join(os.path.dirname(__file__), "lineage-spec.yaml")

aws_arn = get_aws_arn(env)

TtdKubernetesPodOperator(
    namespace='amundsen',
    image=lineage_image,
    name=job_name,
    task_id=job_name,
    dnspolicy='ClusterFirst',
    get_logs=True,
    dag=adag,
    startup_timeout_seconds=500,
    on_finish_action="delete_pod",
    log_events_on_failure=True,
    service_account_name=get_service_account(env),
    annotations={
        'sumologic.com/include': 'true',
        'sumologic.com/sourceCategory': 'amundsen',
        'sumologic.com/sourceName': job_name,
        'iam.amazonaws.com/role': aws_arn,
        'eks.amazonaws.com/role-arn': aws_arn
    },
    cmds=["/bin/bash", "-c"],
    arguments=[f"python dataset_migration_monitoring/main.py --env {ingestion_task_env_str(env)}"],
    pod_template_file=lineage_spec_path,
    queue=Workers.k8s.queue,
    pool=Workers.k8s.pool,
    executor_config=K8sExecutorConfig.watch_task(),
)
