from airflow.listeners import hookimpl
from airflow.models import DagRun
from airflow.utils.state import State

from ttd.metrics.metric_pusher import MetricPusher


def _push_dag_run_status_metric(is_success: bool, dag_run: DagRun):
    metric_pusher = MetricPusher()

    tis = dag_run.task_instances

    total_tis = len(tis)
    successful_tis = sum(1 for ti in tis if ti.state == State.SUCCESS)
    failed_tis = sum(1 for ti in tis if ti.state == State.FAILED)

    labels = {
        'dag_id': dag_run.dag_id,
        'owner': dag_run.get_dag().tasks[0].owner,
        'total_tis': total_tis,
        'successful_tis': successful_tis,
        'failed_tis': failed_tis,
    }

    metric_pusher.push(
        name='dag_run_status',
        value=int(is_success),
        labels=labels,
        timestamp=dag_run.logical_date,
    )


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    _push_dag_run_status_metric(True, dag_run)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    _push_dag_run_status_metric(False, dag_run)
