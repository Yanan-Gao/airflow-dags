from dags.ctxmp import constants
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime

from ttd.slack import slack_groups

job_schedule_interval = "0 1 * * *"
job_start_date = datetime(2025, 3, 3)

dag = TtdDag(
    dag_id="ttd-contextual-blocked-domains",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#scrum-contextual-marketplace-alarms",
    slack_tags=slack_groups.CTXMP.team.name,
    tags=[slack_groups.CTXMP.team.name, slack_groups.CTXMP.team.jira_team],
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_12xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(1024)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(5).with_ebs_size_gb(1024)],
    on_demand_weighted_capacity=100,
)

cluster_task = EmrClusterTask(
    name="TTD_Contextual_Blocked_Domains",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.CTXMP.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    emr_release_label=constants.aws_emr_version,
)

job_task = EmrJobTask(
    name="ctxmp-ttd-contextual-blocked-domains",
    class_name="jobs.contextual.BlockedDomains.TTDContextualBlockedDomains",
    executable_path=constants.eldorado_jar,
    configure_cluster_automatically=True,
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
