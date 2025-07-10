from __future__ import annotations

import logging
from dags.idnt.statics import RunTimes
from ttd.el_dorado.v2.base import TtdDag
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from ttd.tasks.op import OpTask
from datetime import datetime
from typing import Optional, Tuple


class ReleaseBranchHelper:
    """Helper class to provide convenient airflow branching for a release process.
    """

    @staticmethod
    def _find_branch(release_date: datetime, graph_date_str: str, no_task: str, yes_task: str, first_task: Optional[str]):
        graph_date = datetime.strptime(graph_date_str, "%Y-%m-%d")
        logging.info(f"Graph date: {graph_date} Release date: {release_date}")

        if graph_date < release_date:
            logging.info("NO release")
            return no_task
        elif graph_date == release_date and first_task is not None:
            logging.info("FIRST release")
            return first_task
        elif graph_date == release_date and first_task is None:
            logging.info("REGULAR release on first date")
            return yes_task
        else:
            logging.info("REGULAR release")
            return yes_task

    @staticmethod
    def _optional_task_name(task: Optional[OpTask] = None) -> Optional[str]:
        if task:
            return task.first_airflow_op().task_id
        else:
            return None

    @classmethod
    def branch_on_release(
        cls,
        release_date: datetime,
        dag: TtdDag,
        no_release_task: OpTask,
        yes_release_task: OpTask,
        first_release_task: Optional[OpTask] = None,
        task_suffix: str = ""
    ) -> Tuple[OpTask, OpTask]:
        """Branch an airflow DAG based on the DAG execution date and release date.

        We're comparing the .. vs the release_date set here.
        If the release date is later than the `graph_date` then we use the `no_release_task` if
        the same, then we use `first_release_task` if provided otherwise `yes_release_task`. For
        dates after the release date, the `yes_release_task` will be used.

        This returns 2 `OpTask` objects, one to begin branching on and one to finish branching.

        Args:
            release_date (datetime): First release date of dataset created by operators.
            dag (TtdDag): Dag to attend tasks to. Note that we don't attach the tasks directly, but
              this is needed for context.
            no_release_task (OpTask): Task if we're before release date.
            yes_release_task (OpTask): Task if we're after release date.
            first_release_task (Optional[OpTask], optional): Optional task for first release date.
              If None, we use the `yes_release_task`. Defaults to None.
            task_suffix (str, optional): Optional suffix for airflow task naming to avoid conflicts.
              Use this if you're branhcing for multiple clusters. Defaults to "".

        Returns:
            Tuple[OpTask, OpTask]: A start and end task to attach to your dag.
        """

        branch_on_release = OpTask(
            op=BranchPythonOperator(
                task_id=f"branch_on_release{task_suffix}",
                provide_context=True,
                op_kwargs={
                    'release_date': release_date,
                    'graph_date_str': RunTimes.previous_full_day,
                    'no_task': cls._optional_task_name(no_release_task),
                    'yes_task': cls._optional_task_name(yes_release_task),
                    'first_task': cls._optional_task_name(first_release_task)
                },
                python_callable=cls._find_branch,
                dag=dag.airflow_dag
            )
        )

        reconnect = OpTask(
            op=EmptyOperator(
                task_id=f"reconnect_release{task_suffix}", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag.airflow_dag
            )
        )

        # we do this in a specific order so it renders nicely in the DAG
        branch_on_release >> no_release_task >> reconnect
        if first_release_task is not None:
            branch_on_release >> first_release_task >> reconnect

        branch_on_release >> yes_release_task >> reconnect

        return (branch_on_release, reconnect)
