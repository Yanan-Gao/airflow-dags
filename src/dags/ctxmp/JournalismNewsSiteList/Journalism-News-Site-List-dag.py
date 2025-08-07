from dags.ctxmp import constants
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime, timedelta

from ttd.slack import slack_groups

job_schedule_interval = "0 2 * * 6"
job_start_date = datetime(2025, 4, 2)

dag = TtdDag(
    dag_id="ttd-contextual-journalism-news-site-list",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#scrum-contextual-marketplace-alarms",
    slack_tags=slack_groups.CTXMP.team.name,
    tags=[slack_groups.CTXMP.team.name, slack_groups.CTXMP.team.jira_team],
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_12xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(1024)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=100,
)

eldorado_options_list = [('ttd.IntermediateAggregatedBidRequestDataset.isInChain', 'true'),
                         ('ttd.IntermediateAggregatedPrebidDataset.isInChain', 'true')]

cluster_task = EmrClusterTask(
    name="TTD_Contextual_Jouralism_News_Site_List",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.CTXMP.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    emr_release_label=constants.aws_emr_version
)

job_task = EmrJobTask(
    name="ctxmp-ttd-journalism-news-site-list",
    class_name="jobs.contextual.JournalismNewsSiteList.JournalismNewsSiteList",
    executable_path=constants.eldorado_jar,
    configure_cluster_automatically=True,
    timeout_timedelta=timedelta(hours=6),
    eldorado_config_option_pairs_list=eldorado_options_list
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
