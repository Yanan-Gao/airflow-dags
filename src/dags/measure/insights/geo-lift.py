from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack import slack_groups

ttddag = TtdDag(
    dag_id="insights-geo-lift",
    start_date=datetime(2024, 8, 19, 7, 0),
    schedule_interval='0 12 * * *',  # run every day at noon UTC
    depends_on_past=False,
    slack_channel=slack_groups.MEASURE_TASKFORCE_LIFT.team.alarm_channel,
    tags=['measurement']
)

impressions_options = [('date', "{{ ds }}")]
conversions_options = [('date', "{{ ds }}")]

cluster_tags = {
    "process": "geo-lift",
    "Team": slack_groups.MEASURE_TASKFORCE_LIFT.team.jira_team,
}

exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6g_16xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=20
)

impressions_cluster_task = EmrClusterTask(
    name="geolift-impressions-cluster",
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    enable_prometheus_monitoring=True
)

conversions_cluster_task = EmrClusterTask(
    name="geolift-conversions-cluster",
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    enable_prometheus_monitoring=True
)

impressions = EmrJobTask(
    name="impressions",
    class_name="com.thetradedesk.jobs.insights.geolift.ImpressionsDailyAgg",
    eldorado_config_option_pairs_list=impressions_options,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)

conversions = EmrJobTask(
    name="conversions",
    class_name="com.thetradedesk.jobs.insights.geolift.ConversionsDailyAgg",
    eldorado_config_option_pairs_list=conversions_options,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)

impressions_cluster_task.add_sequential_body_task(impressions)
conversions_cluster_task.add_sequential_body_task(conversions)

ttddag >> impressions_cluster_task
ttddag >> conversions_cluster_task
DAG = ttddag.airflow_dag
