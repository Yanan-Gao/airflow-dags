from datetime import datetime, timedelta

from airflow.models import Param
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import PSR
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.vertica_clusters import (
    get_variants_for_group,
    get_cloud_provider_for_cluster,
    TaskVariantGroup,
)

vertica_variant_group = TaskVariantGroup.VerticaEtl
scrum_team = PSR.team
job_schedule_interval = "15 * * * *"
alert_channel = "#scrum-psr-grafana-alerts"
start_date = datetime(2025, 6, 23, 0, 0, 0)
default_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'owner': scrum_team.jira_team,
}
max_active_runs = 1
retries = 2
retry_delay = timedelta(minutes=5)
task_execution_timeout = timedelta(hours=1)

for vertica_cluster in (get_variants_for_group(vertica_variant_group) + get_variants_for_group(TaskVariantGroup.VerticaAliCloud)):
    pipeline_suffix = vertica_cluster.name

    k8s_sovereign_connection_helper = get_cloud_provider_for_cluster(vertica_cluster)

    ttd_dag = TtdDag(
        dag_id="psr-" + TaskServiceOperator.format_task_name('estimated-hourly-potential-spend', pipeline_suffix),
        default_args=default_args,
        schedule_interval=job_schedule_interval,
        slack_channel=alert_channel,
        tags=["TaskService", scrum_team.jira_team],
        max_active_runs=max_active_runs,
        start_date=start_date,
        depends_on_past=False,
        run_only_latest=True,
        params={
            "target_date": Param(datetime.now().strftime("%Y-%m-%d"), type="string"),
            "current_timestamp": Param(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), type="string")
        }
    )

    default_task_args = {
        "task_name_suffix": pipeline_suffix,
        "scrum_team": scrum_team,
        "resources": TaskServicePodResources().small(),
        "retries": retries,
        "retry_delay": retry_delay,
        "task_execution_timeout": task_execution_timeout,
        "vertica_cluster": vertica_cluster,
        "k8s_sovereign_connection_helper": k8s_sovereign_connection_helper,
        "task_concurrency": 1,
        "configuration_overrides": {
            "EstimatedAdGroupHourlyPotentialSpendTaskConfig.CurrentTimestamp": "{{params.current_timestamp}}",
            "EstimatedAdGroupHourlyPotentialSpendTaskConfig.TargetDate": "{{params.target_date}}",
            "SnapshotStatPerHourPerAdGroupMaxVolumeControl.TargetDate": "{{params.target_date}}",
        },
        "telnet_commands": [],
    }

    update_intermediate_table_task = OpTask(
        op=TaskServiceOperator(task_name="UpdateSnapshotStatPerHourPerAdGroupMaxVolumeControlTask", **default_task_args)
    )

    update_estimated_adgroup_hourly_potential_spend = OpTask(
        op=TaskServiceOperator(task_name="UpdateEstimatedAdGroupHourlyPotentialSpend", **default_task_args)
    )

    ttd_dag >> update_intermediate_table_task >> update_estimated_adgroup_hourly_potential_spend
    globals()[ttd_dag.airflow_dag.dag_id] = ttd_dag.airflow_dag
