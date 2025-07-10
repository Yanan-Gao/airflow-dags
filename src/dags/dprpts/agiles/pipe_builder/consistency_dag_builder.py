from datetime import datetime, timedelta
from typing import Optional, Callable

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import dprpts
from dags.dprpts.agiles.pipe_builder.default_args import default_dag_args, default_task_args

scrum_team = dprpts
alert_channel = "#dev-agiles-alerts"
start_date = datetime.now() - timedelta(hours=1)

flow_dag_prefix = "ts-rti-pipe-builder"
consistency_dag_prefix = "ts-rti-pipe-builder"
consistency_dag_suffix = "consistency-check"


def create_consistency_check_dag(flow_name: str) -> TtdDag:
    return TtdDag(
        f"{consistency_dag_prefix}-{flow_name}-{consistency_dag_suffix}",
        schedule_interval=timedelta(hours=1),
        **default_dag_args,
    )


class ConsistencyTaskBuilder:

    def __init__(
        self,
        branch_name: Optional[str] = None,
    ):
        self._branch_name = branch_name

    def create_consistency_check(self, task_config_name: str, task_name_suffix: str) -> OpTask:
        task_args = {"branch_name": self._branch_name, **default_task_args}

        return OpTask(
            op=TaskServiceOperator(
                task_name="VerticaConsistencyCheckTaskOfficialVerticaDriver",
                task_config_name=task_config_name,
                task_name_suffix=task_name_suffix,
                task_execution_timeout=timedelta(hours=3),
                **task_args,
            )
        )


class ConsistencyDagBuilder:

    def __init__(
        self,
        flow_name: str,
        branch_name: Optional[str] = None,
    ):
        self._consistency_dag = create_consistency_check_dag(flow_name)
        self._task_builder = ConsistencyTaskBuilder(branch_name=branch_name)

    def build(self, consistency_dag_builder: Callable[[TtdDag, ConsistencyTaskBuilder], None]) -> TtdDag:
        consistency_dag_builder(self._consistency_dag, self._task_builder)
        return self._consistency_dag
