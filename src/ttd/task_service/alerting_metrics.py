from datetime import datetime, UTC
from typing import Dict

from airflow.models import TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State

from ttd.cloud_provider import CloudProvider
from ttd.slack.slack_groups import SlackTeam
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all
from ttd.ttdprometheus import get_or_register_gauge as get_or_register_gauge_prom
from ttd.ttdprometheus import push_all as push_all_prom
from ttd.ttdenv import TtdEnvFactory

DEV_OPS_GENIE_ID = "Dev"
BLACK_HOLE_OPS_GENIE_ID = "AlertBlackHole"

START_TIME_METRIC = "taskservice_task_start_time"
END_TIME_METRIC = "taskservice_task_end_time"
FAILURE_COUNT_METRIC = "taskservice_task_failure_count"
JOB_NAME = "taskservice_monitoring"


class AlertingMetricsConfiguration:
    """
    Configuration for pushing metrics about TaskService tasks running in Airflow.

    We always push taskservice_task_start_time and taskservice_task_end_time. taskservice_task_failure_count is disabled
    by default. If turned on and the task fails an alert will be triggered.

    :param ignore_failure_metric: When set to False, taskservice_task_failure_count metrics will be pushed.
    :param last_run_only: If false, the value of the metric is the total number of failed tasks. The alert will fire
    as long as this is above 0. If true, the metric is 1 if the most recent run failed, and 0 otherwise. The alert will
    fire as long as the most recent run was a failure.
    :param priority: Priority as appears in Opsplatform.
    :param global_oncall: If true, alert will be routed to global on call.
    """

    def __init__(
        self,
        ignore_failure_metric: bool = True,
        last_run_only: bool = True,
        priority: int = 3,
        global_oncall: bool = False,
    ):
        self.last_run_only = last_run_only
        self.ignore_failure_metric = ignore_failure_metric
        self.priority = priority
        self.global_oncall = global_oncall


class AlertingMetricsMixin:

    def __init__(self, configuration: AlertingMetricsConfiguration):
        self.configuration = configuration
        super().__init__()

    def on_start(
        self,
        context: Dict,
        cloud_provider: CloudProvider,
        jira_key: str,
    ) -> None:
        task_start_time_otel = get_or_register_gauge(
            job=JOB_NAME,
            name=START_TIME_METRIC,
            description="Start time of a TaskService task",
        )

        task_start_time_prom = get_or_register_gauge_prom(
            job=JOB_NAME,
            name=START_TIME_METRIC,
            description="Start time of a TaskService task",
            labels=["dag_id", "scrum_team", "priority"],
        )

        task_start_time_otel.labels({
            "dag_id": context["dag"].dag_id,
            "scrum_team": jira_key,
            "priority": self.configuration.priority,
            "cloud_provider": cloud_provider.__str__(),
            "task_id": context["task"].task_id,
            "app": "OpenTelemetry"
        }).set(datetime.now(UTC).timestamp())

        task_start_time_prom.labels(
            dag_id=context["dag"].dag_id,
            scrum_team=jira_key,
            priority=self.configuration.priority,
        ).set(datetime.now(UTC).timestamp())

    def on_finish(
        self,
        context: Dict,
        cloud_provider: CloudProvider,
        scrum_team: SlackTeam,
        state: State,
    ) -> None:
        task_end_time_otel = get_or_register_gauge(
            job=JOB_NAME,
            name=END_TIME_METRIC,
            description="End time of a TaskService task",
        )
        task_end_time_prom = get_or_register_gauge_prom(
            job=JOB_NAME,
            name=END_TIME_METRIC,
            description="End time of a TaskService task",
            labels=["dag_id", "scrum_team", "priority", "task_state"],
        )

        task_end_time_otel.labels({
            "dag_id": context["dag"].dag_id,
            "scrum_team": scrum_team if isinstance(scrum_team, str) else scrum_team.jira_team.upper(),
            "priority": self.configuration.priority,
            "task_state": state,
            "cloud_provider": cloud_provider.__str__(),
            "task_id": context["task"].task_id,
            "app": "OpenTelemetry"
        }).set(datetime.now(UTC).timestamp())

        task_end_time_prom.labels(
            dag_id=context["dag"].dag_id,
            scrum_team=scrum_team if isinstance(scrum_team, str) else scrum_team.jira_team.upper(),
            priority=self.configuration.priority,
            task_state=state,
        ).set(datetime.now(UTC).timestamp())

        ops_genie_team = self._get_ops_genie_team(scrum_team)

        failure_counter_otel = get_or_register_gauge(
            job=JOB_NAME,
            name=FAILURE_COUNT_METRIC,
            description="Failure count of a TaskService task",
        )

        failure_counter_prom = get_or_register_gauge_prom(
            job=JOB_NAME,
            name=FAILURE_COUNT_METRIC,
            description="Failure count of a TaskService task",
            labels=[
                "dag_id",
                "scrum_team",
                "ignore",
                "task_state",
                "last_run_only",
                "ops_genie_team",
                "priority",
            ],
        )

        failure_counter_otel = failure_counter_otel.labels({
            "dag_id": context["dag"].dag_id,
            "scrum_team": scrum_team.jira_team,
            "ignore": self.configuration.ignore_failure_metric,
            "last_run_only": self.configuration.last_run_only,
            "ops_genie_team": ops_genie_team,
            "priority": self.configuration.priority,
            "cloud_provider": cloud_provider.__str__(),
            "task_id": context["task"].task_id,
            "app": "OpenTelemetry"
        })

        failure_counter_prom = failure_counter_prom.labels(
            dag_id=context["dag"].dag_id,
            scrum_team=scrum_team.jira_team,
            ignore=self.configuration.ignore_failure_metric,
            task_state=state,
            last_run_only=self.configuration.last_run_only,
            ops_genie_team=ops_genie_team,
            priority=self.configuration.priority,
        )

        if self.configuration.last_run_only:
            if state == State.SUCCESS:
                failure_counter_otel.set(0)
                failure_counter_prom.set(0)
            else:
                failure_counter_otel.set(1)
                failure_counter_prom.set(1)
        else:
            failure_counter_otel.set(self._count_failed_tis(context))
            failure_counter_prom.set(self._count_failed_tis(context))

        print("Pushing metrics")

        push_all(job=JOB_NAME)

        push_all_prom(
            job=JOB_NAME,
            grouping_key={
                "env": TtdEnvFactory.get_from_system().execution_env,
                "cloud_provider": cloud_provider.__str__(),
                "task_id": context["task"].task_id,
            },
        )

    def _get_ops_genie_team(self, scrum_team: SlackTeam) -> str:
        if self.configuration.global_oncall:
            return DEV_OPS_GENIE_ID
        else:
            if scrum_team.ops_genie_team is not None:
                return scrum_team.ops_genie_team
            else:
                return BLACK_HOLE_OPS_GENIE_ID

    @provide_session
    def _count_failed_tis(self, context, session=None) -> int:
        return (
            session.query(TaskInstance).filter(TaskInstance.dag_id == context["dag"].dag_id).filter(
                TaskInstance.task_id == context["task"].task_id
            ).filter(TaskInstance.state == State.FAILED).count()
        )
