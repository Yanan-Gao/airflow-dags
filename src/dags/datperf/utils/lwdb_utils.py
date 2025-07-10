from datetime import datetime
from ttd.interop.logworkflow_callables import ExternalGateOpen


def create_fn_lwdb_gate_open(task_batch_grain_daily: int, gating_type_id: int, logworkflow_connection: str = "lwdb"):

    def _get_time_slot(dt: datetime):
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt

    def _open_lwdb_gate(**context):
        dt = _get_time_slot(context['data_interval_start'])
        log_start_time = dt.strftime('%Y-%m-%d %H:00:00')
        ExternalGateOpen(
            mssql_conn_id=logworkflow_connection,
            sproc_arguments={
                'gatingType': gating_type_id,
                'grain': task_batch_grain_daily,
                'dateTimeToOpen': log_start_time
            }
        )

    return _open_lwdb_gate
