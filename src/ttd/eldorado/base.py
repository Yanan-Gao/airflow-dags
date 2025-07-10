from __future__ import annotations
from datetime import timedelta, datetime
from typing import List, Optional, Dict, Callable, TYPE_CHECKING, Any, Union

from airflow import DAG
from airflow.timetables.base import Timetable

from ttd.dag_callback_util import default_on_error_callback, default_on_success_callback
from ttd.rbac.util import format_airflow_role
from ttd.ttdenv import TtdEnv, TtdEnvFactory
from airflow.security import permissions

from ttd.timetables.delayed_timetable import create_delayed_timetable, DelayedIntervalSpec
from airflow.models.dag import ScheduleInterval

if TYPE_CHECKING:
    from ttd.tasks.base import BaseTask

from airflow import settings
import pendulum


class ElDoradoError(Exception):

    def __init__(self, *args):
        super().__init__(*args)


"""
Why another object model? Why OpTask as a wrapper around airflow.models.BaseOperator?

When designing this new API and object model for Airflow we aim for the following goals:

  - streamline construction of a DAG from basic elements (operators)

    With the old model it was non-obvious that an Operator is included into a DAG at the moment
    of *instantiation* and *NOT* at the moment of *binding*. That approach limits the order
    in which DAG elements are defined (e.g. one *must* declare a DAG first, then a SubDAG if needed,
    then an Operator). It also causes issues with programmatic generation of Operator definitions.

    With the new model every element of a DAG can be declared independently and then bound into a DAG
    when it is convenient. In order to allow it BaseTask encapsulates mechanism to store and transfer
    a reference to a DAG to its peers.

  - separate semantic and runtime grouping of operators

    Sometimes it is needed to handle (create and execute) several operators of a DAG as an atomic group.
    With the old model there is an essential way of doing that - a SubDAG. The issue with SubDAG is that
    it introduces grouping both semantic and runtime at the same time. That is whenever we create a group
    of Operators, we create a SubDAG (a separate runtime entity in Airflow), and vice versa.

    Within the new model we introduce two separate concepts for that: ChainOfTasks and SubDagTask.
    So these could be used exactly when needed.

    The ChainOfTasks model introduces new concept of two connection points ('first_airflow_op' and 'last_airflow_op')
    instead of just one in the old model (the Operator itself), so that ChainOfTasks can be used (bound with peers)
    in the same way as a regular task. That is why the base class for all tasks, BaseTask, defines the methods.

Several new classes are introduced to run jobs on EMR cluster within the new concept.
We also created OpTask as a wrapper around regular airflow.models.BaseOperator in order to add to it
the functionality mentioned above and to allow its use together with the new model.

Another neat feature of OpTask allows instantiation of the class (as a part of a logical structure of a DAG)
without defining the operation and then adding the definition later. It is, for example, used in EmrJobTask
where 'add_job_task' and 'watch_job_task' might not be defined at the instantiation time, but get defined later.
"""


class TtdDag:

    def __init__(
        self,
        dag_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        schedule_interval: Union[ScheduleInterval, Timetable, DelayedIntervalSpec] = None,
        # If depends_on_past=True then future task instances will block until previous instances
        # are successful, i.e. needs human intervention. Use False only if the workflow doesn't
        # depend on output from previous instances.
        depends_on_past: bool = False,
        run_only_latest: Optional[bool] = None,
        max_active_tasks: int = 16,
        max_active_runs: int = 1,
        retries: int = 0,
        retry_delay: timedelta = timedelta(minutes=15),
        contact_email: bool = None,
        enable_slack_alert: bool = True,
        slack_channel: Optional[str] = None,
        slack_tags: Optional[str] = None,
        dag_tsg: Optional[str] = None,
        tags: Optional[List[str]] = None,
        default_args: Dict = None,
        slack_alert_only_for_prod: bool = True,
        on_failure_callback: Optional[Callable] = None,
        on_success_callback: Optional[Callable] = None,
        dagrun_timeout: timedelta = timedelta(hours=24),
        params: Optional[Dict[str, Any]] = None,
        teams_allowed_to_access: Optional[List[str]] = None,
    ):
        """

        :param run_only_latest: Whether scheduler should run (catchup) all missed execution form the start date.
            If None (default), will depend on the Environment: 'prod' - False (catchup), otherwise - True (run_only_latest).
            True - run only latest (do not catchup).
            False - run everything from start date (catchup).
        :param enable_slack_alert: If True (default) sends Slack notification
        :param max_active_tasks: Number of TIs allowed to run concurrently across all DagRuns in a DAG
        :param max_active_runs: Number of DagRuns allowed to run concurrently
        """

        self._dag_spec = locals()
        del self._dag_spec["self"]

        tags = tags or []
        _default_args = {
            "depends_on_past": depends_on_past,
            "start_date": start_date,
            "end_date": end_date,
            "email": [contact_email] if contact_email else [],  # Where to send failure and retry e-mails to
            "email_on_failure": contact_email is not None,
            "email_on_retry": False,
            "retries": retries,
            "retry_delay": retry_delay
        }

        _default_args.update(default_args or {})

        self.slack_configuration = SlackConfiguration(
            slack_channel,
            slack_tags,
            dag_tsg,
            enable_slack_alert,
            slack_alert_only_for_prod,
            on_failure_callback,
            on_success_callback,
        )

        catchup = \
            (True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False) \
            if run_only_latest is None else not run_only_latest

        if isinstance(schedule_interval, DelayedIntervalSpec):
            if start_date is not None and start_date.tzinfo is not None:
                tz = pendulum.instance(start_date, tz=start_date.tzinfo).timezone
            else:
                tz = settings.TIMEZONE
            schedule = create_delayed_timetable(schedule_interval, tz, dag_id=dag_id)
        else:
            schedule = schedule_interval

        self._airflow_dag = DAG(
            dag_id=dag_id,
            # These are applied to all operators. See link below for description of all
            # parameters that can be applied to BaseOperator.
            #
            # https://airflow.apache.org/_api/airflow/models/index.html#airflow.models.BaseOperator
            default_args=_default_args,
            schedule=schedule,
            max_active_tasks=max_active_tasks,
            max_active_runs=max_active_runs,
            catchup=catchup,  # (optional) Allow missed/old scheduled runs? If False, only latest
            dagrun_timeout=dagrun_timeout,  # (optional) Don't allow it to run longer
            on_failure_callback=self.slack_configuration.into_error_callback(dag_id),
            on_success_callback=self.slack_configuration.into_success_callback(),
            tags=tags,
            start_date=start_date,
            end_date=end_date,
            params=params,
            access_control=self.map_teams_to_access_control(teams_allowed_to_access)
            if TtdEnvFactory.get_from_system() != TtdEnvFactory.prodTest else None,
        )

        globals()[self._airflow_dag.dag_id] = self._airflow_dag
        self._downstream: List["BaseTask"] = []

    @property
    def airflow_dag(self) -> DAG:
        return self._airflow_dag

    @property
    def downstream(self) -> List["BaseTask"]:
        return self._downstream

    def set_downstream(self, others: BaseTask | list[BaseTask]):
        if not isinstance(others, list):
            others = [others]

        for other in others:
            if not other:
                raise ElDoradoError("other must not be None")

            if other not in self._downstream:
                self._downstream.append(other)

            other._set_ttd_dag(self)

    def __rshift__(self, others: BaseTask | list[BaseTask]) -> BaseTask | list[BaseTask]:
        self.set_downstream(others)
        return others

    @staticmethod
    def map_teams_to_access_control(team_names: Optional[List[str]]) -> dict | None:
        if team_names is None:
            return None
        return {format_airflow_role(team): {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT} for team in team_names}


class ClusterSpecs:

    def __init__(
        self,
        cluster_name: str,
        environment: TtdEnv,
    ):
        self.cluster_name = cluster_name
        self.environment = environment


class SlackConfiguration:

    def __init__(
        self,
        slack_channel: Optional[str] = None,
        slack_tags: Optional[str] = None,
        dag_tsg: Optional[str] = None,
        enable_slack_alert: bool = True,
        slack_alert_only_for_prod: bool = True,
        on_failure_callback: Optional[Callable] = None,
        on_success_callback: Optional[Callable] = None,
    ):
        self.slack_channel = slack_channel
        self.slack_tags = slack_tags
        self.dag_tsg = dag_tsg
        self.enable_slack_alert = enable_slack_alert
        self.slack_alert_only_for_prod = slack_alert_only_for_prod
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback

    def into_error_callback(self, dag_id: str):
        return default_on_error_callback(
            dag_id,
            self.slack_channel,
            self.slack_tags,
            self.dag_tsg,
            self.enable_slack_alert,
            self.slack_alert_only_for_prod,
            self.on_failure_callback,
        )

    def into_success_callback(self):
        return default_on_success_callback(self.on_success_callback)
