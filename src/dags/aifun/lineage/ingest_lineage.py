import os
from datetime import timedelta, datetime
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.ttdenv import TtdEnvFactory
from typing import List

from ttd.workers.worker import Workers

from dags.aifun.lineage.constants import MARQUEZ_ENDPOINTS, get_aws_arn, get_service_account

# Global job constants defined here (shared across all lineage tasks)s
latest_prod_ingestion_image = 'production.docker.adsrvr.org/ttd-base/mlops/thetradedesk_lineage:latest'

env = TtdEnvFactory.get_from_system()

# was 16
job_start_date = datetime(2024, 6, 24, 16, 0)

hour_lookback = 1

dag = TtdDag(
    dag_id="lineage-ingestion",
    start_date=job_start_date,
    schedule_interval=timedelta(hours=hour_lookback),
    slack_tags="AIFUN",
    max_active_runs=1
)
adag = dag.airflow_dag
lineage_spec_path = os.path.join(os.path.dirname(__file__), "lineage-spec.yaml")

staleness_threshold = "{{ dag_run.conf.get('staleness_threshold', 50) }}"


class IngestionTaskConfig:
    name: str
    app_name: str
    sumo_category: str
    sumo_source: str
    extra_args: List[str]
    date_based: bool
    is_long_running: bool

    def __init__(
        self,
        name: str,
        app_name: str,
        sumo_category: str,
        sumo_source: str,
        extra_args: List[str],
        date_based: bool = True,
        is_long_running: bool = False,
    ):
        self.name = name
        self.app_name = app_name
        self.sumo_category = sumo_category
        self.sumo_source = sumo_source
        self.extra_args = extra_args
        self.date_based = date_based
        self.is_long_running = is_long_running


def get_ingestion_task(task_config: IngestionTaskConfig, task_dag):
    args = [f"app/{task_config.app_name}.py", f"--env={'prod' if env.execution_env == 'prod' else 'dev'}"]
    if task_config.date_based:
        args += ["--time={{data_interval_end.strftime(\"%Y-%m-%dT%H:00:00\")}}Z"]
    args += task_config.extra_args
    aws_arn = get_aws_arn(env)
    return TtdKubernetesPodOperator(
        namespace='amundsen',
        image=latest_prod_ingestion_image,
        name=task_config.name,
        task_id=task_config.name,
        dnspolicy='ClusterFirst',
        get_logs=True,
        dag=task_dag,
        startup_timeout_seconds=500,
        on_finish_action="delete_pod",
        log_events_on_failure=True,
        service_account_name=get_service_account(env),
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': task_config.sumo_category,
            'sumologic.com/sourceName': task_config.sumo_source,
            'iam.amazonaws.com/role': aws_arn,
            'eks.amazonaws.com/role-arn': aws_arn
        },
        cmds=["python3"],
        arguments=args,
        env_vars=dict(MARQUEZ_URL=MARQUEZ_ENDPOINTS.raw, NORMALIZED_PROXY_URL=MARQUEZ_ENDPOINTS.normalized),
        pod_template_file=lineage_spec_path,
        queue=Workers.k8s.queue if task_config.is_long_running else Workers.celery.queue,
        pool=Workers.k8s.pool if task_config.is_long_running else Workers.celery.pool,
        executor_config=K8sExecutorConfig.watch_task() if task_config.is_long_running else None,
    )


# Lineage tasks defined here
ROBUST_TIMEOUT_IN_MINS = 2

upload_events_step = get_ingestion_task(
    IngestionTaskConfig(
        name="sync_to_marquez",
        app_name="sync_to_marquez",
        sumo_category="mlops/openlineage",
        sumo_source="amundsen-ingestion",
        extra_args=
        [f"--hours-look-back={hour_lookback}", f"--timeout={ROBUST_TIMEOUT_IN_MINS * 60}", "--skip-data-mover", "--skip-vertica-views"],
        is_long_running=True
    ),
    task_dag=adag
)

ingest_to_db_step = get_ingestion_task(
    IngestionTaskConfig(
        name="snapshot_marquez_and_ingest_to_neo4j",
        app_name="ingest_to_db",
        sumo_category="amundsen",
        sumo_source="amundsen-ingestion",
        extra_args=[f"--staleness-threshold={staleness_threshold}"],
        is_long_running=True,
    ),
    task_dag=adag
)

update_es_index_step = get_ingestion_task(
    IngestionTaskConfig(
        name="update_es_index",
        app_name="create_es_index",
        sumo_category="amundsen",
        sumo_source="amundsen-ingestion",
        extra_args=[],
        date_based=False
    ),
    task_dag=adag
)

upload_events_step >> ingest_to_db_step >> update_es_index_step
