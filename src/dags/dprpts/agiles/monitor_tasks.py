from datetime import datetime, timedelta

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.vertica_clusters import get_variants_for_group, TaskVariantGroup

base_task_name = "ts-rti-"

vertica_variant_group = TaskVariantGroup.VerticaAws
scrum_team = dprpts
job_schedule_interval = "*/30 * * * *"
alert_channel = "#dev-agiles-alerts"
start_date = datetime.now() - timedelta(hours=1)
default_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'owner': scrum_team.jira_team,
}
max_active_runs = 1
retries = 2
retry_delay = timedelta(minutes=5)
task_execution_timeout = timedelta(hours=3)

for vertica_cluster in get_variants_for_group(vertica_variant_group):
    pipeline_suffix = vertica_cluster.name
    # 1 dag per vertica cluster
    ttd_dag = TtdDag(
        base_task_name + TaskServiceOperator.format_task_name("pipe-monitor-task", pipeline_suffix),
        default_args=default_args,
        schedule_interval=job_schedule_interval,
        slack_channel=alert_channel,
        tags=["TaskService", scrum_team.jira_team],
        max_active_runs=max_active_runs,
        start_date=start_date,
        depends_on_past=False,
        run_only_latest=False,
    )

    secret_suffix = "-" + vertica_cluster.name.replace("01", "").lower()

    default_task_args = {
        "task_name_suffix": pipeline_suffix,
        "scrum_team": scrum_team,
        "resources": TaskServicePodResources().small(),
        "retries": retries,
        "retry_delay": retry_delay,
        "task_execution_timeout": task_execution_timeout,
        "vertica_cluster": vertica_cluster,
        "configuration_overrides": {
            "RtiPipeMonitorTask.OverrideConfigViaSecret": "test/use-dataops003-credentials" + secret_suffix
        },
    }

    monitor_task = OpTask(op=TaskServiceOperator(task_name="RtiPipeMonitorTask", **default_task_args))

    ttd_dag >> monitor_task

    globals()[ttd_dag.airflow_dag.dag_id] = ttd_dag.airflow_dag
