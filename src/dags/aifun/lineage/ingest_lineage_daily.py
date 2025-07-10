import os
from datetime import timedelta, datetime

from dags.aifun.lineage.ingest_lineage import get_ingestion_task, IngestionTaskConfig
from ttd.el_dorado.v2.base import TtdDag
from ttd.ttdenv import TtdEnvFactory

# Global job constants defined here (shared across all lineage tasks)
latest_prod_ingestion_image = 'production.docker.adsrvr.org/ttd-base/mlops/thetradedesk_lineage:latest'

env = TtdEnvFactory.get_from_system()

job_start_date = datetime(2025, 6, 3, 16, 0)

hour_lookback = 1

dag = TtdDag(
    dag_id="lineage-ingestion-daily", start_date=job_start_date, schedule_interval=timedelta(days=1), slack_tags="AIFUN", max_active_runs=1
)
adag = dag.airflow_dag
lineage_spec_path = os.path.join(os.path.dirname(__file__), "lineage-spec.yaml")

ROBUST_TIMEOUT_IN_MINS = 2
upload_events_daily_step = get_ingestion_task(
    IngestionTaskConfig(
        name="sync_to_marquez_daily",
        app_name="sync_to_marquez",
        sumo_category="mlops/openlineage",
        sumo_source="amundsen-ingestion",
        extra_args=
        [f"--hours-look-back={hour_lookback}", f"--timeout={ROBUST_TIMEOUT_IN_MINS * 60}", "--skip-schema-parser", "--skip-robust"],
    ),
    task_dag=adag
)
upload_events_daily_step
