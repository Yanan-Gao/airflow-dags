import logging
from enum import StrEnum, auto, verify, UNIQUE
from typing import Optional

from ttd.metrics.metric_db_client import MetricDBClient
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

from cachetools.func import ttl_cache

from ttd.ttdenv import TtdEnvFactory


@verify(UNIQUE)
class SecretsSource(StrEnum):
    KUBERNETES = auto()
    VAULT = auto()


@verify(UNIQUE)
class SecretsRequestStatus(StrEnum):
    SUCCESS = auto()
    ERROR = auto()


class SecretsAccessMetrics(MetricDBClient):

    def __init__(self, **kwargs):
        super().__init__(log_level=logging.WARNING, **kwargs)

    SECRET_ACCESS_INSERT_SPROC = 'metrics.insert_secrets_access'

    AIRFLOW_METRICS_DB_CONN_ID = 'airflow_metrics_pusher'

    INSERT_METRIC_QUERY = """
            INSERT INTO airflow_metrics.secrets_access (team_name, source, status, request_duration_ms, timestamp)
            VALUES (%(team_name)s, %(source)s, %(status)s, %(request_duration_ms)s, %(timestamp)s);
        """

    @ttl_cache(maxsize=1, ttl=60)
    def _is_monitoring_enabled(self):
        from airflow.models import Variable
        return Variable.get("SECRETS_BACKEND_MONITORING_ENABLED", deserialize_json=True, default_var=True)

    def _send_metrics_airflow_db(
        self,
        source: SecretsSource,
        request_status: SecretsRequestStatus,
        request_duration_ms: float,
        team_name: Optional[str] = None
    ) -> None:
        """
        Legacy method of pushing metrics to Airflow DB.
        """
        pg_hook = PostgresHook(
            postgres_conn_id=self.AIRFLOW_METRICS_DB_CONN_ID,
            log_sql=False,
            # TODO - uncomment this when the AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS config is changed to log_config.LOGGING_CONFIG
            # logger_name='postgres'
        )

        metric_details = {
            'team_name': team_name,
            'source': source.value,
            'status': request_status.value,
            'request_duration_ms': request_duration_ms,
            'timestamp': datetime.now()
        }

        pg_hook.run(SecretsAccessMetrics.INSERT_METRIC_QUERY, parameters=metric_details, autocommit=True)

    def send_metrics(
        self,
        source: SecretsSource,
        request_status: SecretsRequestStatus,
        start_time: float,
        end_time: float,
        team_name: Optional[str] = None
    ) -> None:

        if self._is_monitoring_enabled:

            request_duration_ms = int((end_time - start_time) * 1000)
            parameters = [team_name, source.value, request_status.value, request_duration_ms, datetime.now()]

            self.execute_sproc(SecretsAccessMetrics.SECRET_ACCESS_INSERT_SPROC, parameters)

            if not TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
                self._send_metrics_airflow_db(source, request_status, request_duration_ms, team_name)
