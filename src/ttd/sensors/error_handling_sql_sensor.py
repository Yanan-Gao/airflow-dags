import logging
from datetime import datetime

from airflow.providers.common.sql.sensors.sql import SqlSensor
from pymssql._pymssql import OperationalError
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all


class ErrorHandlingSqlSensor(SqlSensor):
    ERROR_METRIC = 1
    SUCCESS_METRIC = 0

    def __init__(self, conn_id: str, sql: str, *args, **kwargs):
        super().__init__(conn_id=conn_id, sql=sql, *args, **kwargs)

    def poke(self, context):
        data_interval_start = context['data_interval_start']
        try:
            result = super().poke(context)
        except OperationalError as ex:
            logging.error(f'Poke operation failed: {ex}')
            self.send_metric(self.ERROR_METRIC, data_interval_start)
            return False

        self.send_metric(self.SUCCESS_METRIC, data_interval_start)
        return result

    def send_metric(self, value: int, execution_date: datetime) -> None:
        sql_errors = get_or_register_gauge(job=self.dag_id, name="airflow_sql_sensor_error", description="Error during poke operation")
        sql_errors.labels({"execution_date": str(execution_date)}).set(value)
        push_all(self.dag_id)
