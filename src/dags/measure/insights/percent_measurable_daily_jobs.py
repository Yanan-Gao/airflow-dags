from datetime import datetime

from airflow.models import Param

from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.ttdenv import TtdEnvFactory

job_schedule_interval = "0 12 * * *"
job_start_date = datetime(2024, 8, 28)
exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

spark_options = [("conf", "spark.driver.maxResultSize=0"), ("conf", "spark.kryoserializer.buffer.max=2047m")]

cluster_tags = {
    "Team": slack_groups.MEASURE_TASKFORCE_LIFT.team.jira_team,
}

# TODO: param defaults are in the spark job, but jinja always returns a string value of the param (i.e. None value will
#  render as "None"), which is why we need real default values and iterate_from_inclusive (int) is not in this list
PARAM_DEFAULTS = {
    "run_date": datetime.today().strftime("%Y-%m-%d"),
    "dry_run": "false",
    "from_scratch": "false",
    "output_folder": f"s3a://ttd-insights/datapipeline/{TtdEnvFactory.get_from_system()}/measurability-jobs",
}

dag = TtdDag(
    dag_id="measurability-jobs",
    run_only_latest=True,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#taskforce-lift-alarms",
    slack_tags=slack_groups.MEASURE_TASKFORCE_LIFT.team,
    tags=["measurement", "percent-measurable-daily-jobs"],
    params={
        "run_date": Param(PARAM_DEFAULTS["run_date"], type="string", format="date"),
        "dry_run": Param(PARAM_DEFAULTS["dry_run"], type="string", enum=["true", "false"]),
        "from_scratch": Param(PARAM_DEFAULTS["from_scratch"], type="string", enum=["true", "false"]),
        "output_folder": Param(PARAM_DEFAULTS["output_folder"], type="string"),
    },
)

adag = dag.airflow_dag

eldorado_config_option_pairs_list = [(key, f"{{{{ dag_run.conf.get('{key}', '{PARAM_DEFAULTS[key]}') }}}}") for key in adag.params.keys()]

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6g_4xlarge().with_ebs_size_gb(1000).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6g_16xlarge().with_ebs_size_gb(3000).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=100,
)

universe_cluster_task = EmrClusterTask(
    name="measurability-universe-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

impressions_cluster_task = EmrClusterTask(
    name="measurability-impressions-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

export_cluster_task = EmrClusterTask(
    name="measurability-export-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

universe_job_task = EmrJobTask(
    name="measurability-universe-task",
    class_name="com.thetradedesk.jobs.insights.measurability.MeasurabilityUniverse",
    executable_path=exec_jar,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
)

impressions_job_task = EmrJobTask(
    name="measurability-impressions-task",
    class_name="com.thetradedesk.jobs.insights.measurability.MeasurabilityImpressions",
    executable_path=exec_jar,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
    additional_args_option_pairs_list=spark_options,
)

export_job_task = EmrJobTask(
    name="measurability-export-task",
    class_name="com.thetradedesk.jobs.insights.measurability.MeasurabilityExport",
    executable_path=exec_jar,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
)

universe_cluster_task.add_sequential_body_task(universe_job_task)
impressions_cluster_task.add_sequential_body_task(impressions_job_task)
# export_cluster_task.add_sequential_body_task(export_job_task)

# dag >> universe_cluster_task >> impressions_cluster_task >> export_cluster_task
dag >> universe_cluster_task >> impressions_cluster_task
