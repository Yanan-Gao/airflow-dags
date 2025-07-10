from datetime import timedelta, datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import OPATH
from ttd.interop.vertica_import_operators import VerticaImportFromCloud, LogTypeFrequency
from ttd.ttdenv import TtdEnvFactory

####################################################################################################################
# DAG
####################################################################################################################

job_start_date = datetime(2024, 7, 10, 12, 0, 0)
schedule_interval_daily = "0 3 * * *"
max_retries: int = 3
retry_delay: timedelta = timedelta(minutes=20)

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-openpath-assembly.jar"
job_environment = TtdEnvFactory.get_from_system()

dag_id = "openpath-daily-avails-cached-processing"
if job_environment != TtdEnvFactory.prod:
    dag_id = "openpath-daily-avails-cached-processing-test"

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=schedule_interval_daily,
    retries=max_retries,
    retry_delay=retry_delay,
    slack_tags=OPATH.team.jira_team,
    slack_channel=OPATH.team.alarm_channel,
    enable_slack_alert=False
)

####################################################################################################################
# clusters
####################################################################################################################

cluster_task_name = "OpenPathDailyAvailsCachedProcessing"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_9xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

std_core_instance_types = [
    C5.c5_9xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(9),
    C5.c5_12xlarge().with_ebs_size_gb(384).with_max_ondemand_price().with_fleet_weighted_capacity(12),
    C5.c5_18xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(18),
]

core_fleet_capacity = 180

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

standard_cluster_tags = {'Team': OPATH.team.jira_team, "Process": "OpenPath-Daily-Avails-Cached-Processing"}

aws_region = "us-east-1"

daily_cluster_task = EmrClusterTask(
    name=cluster_task_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags=standard_cluster_tags,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    environment=job_environment,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    region_name=aws_region
)

####################################################################################################################
# steps
####################################################################################################################

job_class_name = "jobs.openpath.SampledAvailsCached"

spark_options_list = [("executor-memory", "15G"), ("executor-cores", "10"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=40G"),
                      ("conf", "spark.driver.maxResultSize=6G")]

ResultsRoot = "s3://ttd-openpath-general/spark/{0}/parquet/cached/".format(job_environment.execution_env)
AvailsRoot = "s3://thetradedesk-useast-logs-2/avails/cleansed/"
date_macro = "{{ logical_date.strftime(\"%Y-%m-%d\") }}"

eldorado_config_option_pairs_list = [("resultsRoot", ResultsRoot), ("availsRoot", AvailsRoot), ("date", date_macro)]

job_step = EmrJobTask(
    name="ExtractCachedValuesFromAvails",
    executable_path=jar_path,
    class_name=job_class_name,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)

FIRST_PARTY_DATA_GATING_TYPE_ID = 2000227  # ImportOpenPathFirstPartyData
PLACEMENT_ID_DATA_GATING_TYPE_ID = 2000233  # ImportOpenPathPlacementIdData

kvp_open_gate_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="ExtractCachedValuesFromAvails_OpenLWGate_KVP",
    gating_type_id=FIRST_PARTY_DATA_GATING_TYPE_ID,
    log_type_frequency=LogTypeFrequency.DAILY,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

placement_id_open_gate_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="ExtractCachedValuesFromAvails_OpenLWGate_PlacementId",
    gating_type_id=PLACEMENT_ID_DATA_GATING_TYPE_ID,
    log_type_frequency=LogTypeFrequency.DAILY,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

job_step >> kvp_open_gate_task
job_step >> placement_id_open_gate_task

daily_cluster_task.add_parallel_body_task(job_step)
dag >> daily_cluster_task

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
adag = dag.airflow_dag
