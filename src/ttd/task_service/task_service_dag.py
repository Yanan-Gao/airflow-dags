from datetime import datetime, timedelta
from typing import Optional, List, Dict, Iterable

from airflow.models.dag import ScheduleInterval

from ttd.eldorado.base import TtdDag
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
)
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import SlackTeam
from ttd.task_service.alerting_metrics import AlertingMetricsConfiguration
from ttd.task_service.k8s_connection_helper import aws, K8sSovereignConnectionHelper
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.kubernetes.pod_resources import PodResources
from ttd.task_service.vertica_clusters import (
    TaskVariantGroup,
    get_variants_for_group,
    VerticaCluster,
)
from ttd.timetables.delayed_timetable import DelayedIntervalSpec


class TaskServiceDagFactory:
    """Class that is used to generate a DAG that will run a task service task with the specified parameters.

    For an introduction to Airflow concepts around scheduling, including ``depends_on_past``,
    ``job_schedule_interval`` and ``backfill``, see this Airflow
    `documentation <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html>`__,

    Here is a typical production example::

        TaskServiceDagFactory(
            task_name="DatalakeConsistencyCheckTask",
            scrum_team=dataproc,
            task_config_name="HourlyDatalakeConsistencyCheckConfig",
            task_name_suffix="hourly",
            start_date=datetime(2023, 2, 9),
            job_schedule_interval="*/15 * * * *",
            resources=TaskServicePodResources.medium()
        ),

    All the required parameters are provided as well as a ``task_name_suffix`` as this task class is used more than once
    with different config classes.


    Here's an example for a typical branch deployment::

        TaskServiceDagFactory(
            task_name="ProvisioningToParquetChangeTrackingTask",
            scrum_team=dataproc,
            task_config_name="ProvisioningToParquetChangeTrackingTaskConfig",
            branch_name="alb-DATAPROC-3000-migrate-my-task",
            start_date=datetime(2023, 2, 9),
            job_schedule_interval="*/15 * * * *",
            resources=TaskServicePodResources.large()
        ),

    As we are running a branch deployment, we provide the mandatory ``branch_name`` parameter.

    :param task_name: Corresponds to the name of the task class to be run in Adplatform.
    :param scrum_team: The team that owns the task. The jira_team field of this must correspond to the scrum team
        defined in the task's metadata in Adplatform.
    :param start_date: The date Airflow will start scheduling task from. See also - catchup
    :param job_schedule_interval: The interval at which Airflow runs the task. This should be provided as a cron
        expression.
    :param task_config_name: Corresponds to the name of the task's config class in Adplatform.
    :param task_data: Arbitrary string that can be passed to your config class. To use, you should override the ParseTaskData
        method that exists on the base config class.
    :param task_name_suffix: Airflow requires each DAG to have a unique name. By default, the DAG name is the task_name.
        If you have multiple DAGs with the same task but different configs, you should give them different suffixes here.
    :param depends_on_past: When set to true, prevents a new execution of the task from being executed if a previous one
        has failed.
    :param run_only_latest: By default, we just run the next interval scheduled. If there has been a delay in task
    execution, we ignore execution dates in the past and skip past them. If set to false, if Airflow sees any
    intervals between the DAG start time and now for which we do not have task executions, it will execute a task run
     for that interval.
    :param branch_name: Only applicable on pre-prod Airflow. Runs the task using the name of a branch used in a branch
        deployment.
    :param cloud_provider: The cloud provider where the Kubernetes pod will be run.
    :param resources: The resource requests and limits the pod shall be constrained by. See PodResources for details.
    :param alerting_configuration: The alerting configuration for this task. See AlertingMetricsConfiguration for details.
    :param retries: The number of times Airflow will re-run a task execution after a failure.
    :param retry_delay: The delay between retries.
    :param max_active_runs: The number of concurrent runs of the same task that Airflow will allow.
    :param task_execution_timeout: The time a single task run will execute for before Airflow kills the pod and marks
    that run as failed. By default, this is an hour.
    :param configuration_overrides: Allow values in the ConfigurationOverrides.yaml file to be overwritten.
    :param telnet_commands: Telnet commands that will be run at pod initialisation.
    :param alert_channel: By default, Slack alerts on failure are sent to the channel of the owning team. This allows
        the channel to be specified for the given task.
    :param enable_slack_alert: By default, Slack alerts on failure are enabled. This allows to disable Slack alerts for
        the given task.
    :param vertica_cluster: For tasks that support this, this allows a vertica cluster to be provided to the class.
    :param slack_tags: Used to tag specific slack group or a person when alarm is posted.
    :param dag_tags: Custom tags to add to the airflow dag created for the task service task
    :param k8s_annotations: Custom annotations (key, value) to add to the k8s pods created for the task service task
    """

    _DELAY_MAX_DATA_INTERVAL_PROPORTION = 0.2
    _DELAY_MAX_MINUTES = 30

    def __init__(
        self,
        task_name: str,
        scrum_team: SlackTeam,
        start_date: datetime,
        job_schedule_interval: Optional[ScheduleInterval] = None,
        task_config_name: Optional[str] = None,
        task_data: Optional[str] = None,
        task_name_suffix: Optional[str] = None,
        depends_on_past: bool = False,
        run_only_latest: bool = True,
        branch_name: Optional[str] = None,
        cloud_provider: K8sSovereignConnectionHelper = aws,
        resources: PodResources = TaskServicePodResources.medium(),
        persistent_storage_config: Optional[PersistentStorageConfig] = None,
        alerting_configuration: Optional[AlertingMetricsConfiguration] = AlertingMetricsConfiguration(),
        retries: int = 2,
        retry_delay: timedelta = timedelta(minutes=5),
        max_active_runs: int = 1,
        task_execution_timeout: timedelta = timedelta(hours=1),
        configuration_overrides: Optional[Dict[str, str]] = None,
        telnet_commands: Optional[List[str]] = None,
        alert_channel: Optional[str] = None,
        enable_slack_alert: bool = True,
        vertica_cluster: Optional[VerticaCluster] = None,
        slack_tags: Optional[str] = None,
        dag_tags: Optional[List[str]] = None,
        k8s_annotations: Optional[Dict[str, str]] = None,
        dag_tsg: Optional[str] = None,
        teams_allowed_to_access: Optional[List[str]] = None,
        service_name: Optional[str] = None
    ):
        self.task_name = task_name
        self.scrum_team = scrum_team
        self.start_date = start_date
        self.depends_on_past = depends_on_past
        self.run_only_latest = run_only_latest
        self.task_config_name = task_config_name
        self.task_data = task_data
        self.task_name_suffix = task_name_suffix
        self.branch_name = branch_name
        self.k8s_sovereign_connection_helper = cloud_provider
        self.resources = resources
        self.persistent_storage_config = persistent_storage_config
        self.alerting_configuration = alerting_configuration
        self.retries = retries
        self.retry_delay = retry_delay
        self.max_active_runs = max_active_runs
        self.task_execution_timeout = task_execution_timeout
        self.configuration_overrides = configuration_overrides
        self.telnet_commands = telnet_commands
        self.alert_channel = alert_channel
        self.enable_slack_alert = enable_slack_alert
        self.vertica_cluster = vertica_cluster
        self.slack_tags = slack_tags
        self.dag_tags = dag_tags or []
        self.k8s_annotations = k8s_annotations or {}
        self.dag_tsg = dag_tsg or None
        self.teams_allowed_to_access = teams_allowed_to_access
        self.service_name = service_name

        if job_schedule_interval is not None:
            self.job_schedule_interval = DelayedIntervalSpec(
                interval=job_schedule_interval,
                delay_max_proportion=self._DELAY_MAX_DATA_INTERVAL_PROPORTION,
                max_delay=timedelta(minutes=self._DELAY_MAX_MINUTES)
            )
            self.dag_tags.append('timetable:Delayed')
        else:
            self.job_schedule_interval = None

    def create_dag(self):
        airflow_pipeline_id = TaskServiceOperator.format_task_name(self.task_name, self.task_name_suffix)

        default_args = {
            "start_date": self.start_date,
            "email_on_failure": False,
            "email_on_retry": False,
            "owner": self.scrum_team.jira_team,
        }

        dag = TtdDag(
            "ts-" + airflow_pipeline_id,
            default_args=default_args,
            schedule_interval=self.job_schedule_interval,
            slack_channel=self.scrum_team.alarm_channel if self.alert_channel is None else self.alert_channel,
            enable_slack_alert=self.enable_slack_alert,
            slack_tags=self.slack_tags,
            tags=self.dag_tags,
            max_active_runs=self.max_active_runs,
            start_date=self.start_date,
            depends_on_past=self.depends_on_past,
            run_only_latest=self.run_only_latest,
            dag_tsg=self.dag_tsg,
            teams_allowed_to_access=self.teams_allowed_to_access,
        )

        tso = TaskServiceOperator(
            task_name=self.task_name,
            task_config_name=self.task_config_name,
            task_name_suffix=self.task_name_suffix,
            task_data=self.task_data,
            branch_name=self.branch_name,
            scrum_team=self.scrum_team,
            k8s_sovereign_connection_helper=self.k8s_sovereign_connection_helper,
            resources=self.resources,
            persistent_storage_config=self.persistent_storage_config,
            alerting_configuration=self.alerting_configuration,
            retries=self.retries,
            retry_delay=self.retry_delay,
            task_execution_timeout=self.task_execution_timeout,
            telnet_commands=self.telnet_commands,
            configuration_overrides=self.configuration_overrides,
            vertica_cluster=self.vertica_cluster,
            k8s_annotations=self.k8s_annotations,
            service_name=self.service_name,
        )

        dag >> OpTask(op=tso)

        return dag


class TaskServiceDagRegistry:
    """Class that enables registering of TaskService dags in airflow

    :param global_vars: the result of calling `globals()` - the output of the TaskServiceDagFactory is stored here so airflow will pickup the dag
    """

    def __init__(self, global_vars):
        self.global_vars = global_vars

    def register_dag(self, task_service_dag_factory: TaskServiceDagFactory):
        dag = task_service_dag_factory.create_dag()
        self.global_vars[dag.airflow_dag.dag_id] = dag.airflow_dag

    def register_dags(self, task_service_dag_factories: Iterable[TaskServiceDagFactory]):
        for ts_director in task_service_dag_factories:
            self.register_dag(ts_director)


def create_all_for_vertica_variant_group(vertica_variant_group: TaskVariantGroup, **kwargs) -> List[TaskServiceDagFactory]:
    extra_suffix = ""
    if "task_name_suffix" in kwargs:
        extra_suffix += kwargs["task_name_suffix"]
    for vertica_cluster in get_variants_for_group(vertica_variant_group):
        if extra_suffix == "":
            kwargs["task_name_suffix"] = vertica_cluster.name
        else:
            kwargs["task_name_suffix"] = extra_suffix + "-" + vertica_cluster.name
        yield TaskServiceDagFactory(vertica_cluster=vertica_cluster, **kwargs)
