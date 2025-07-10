from datetime import timedelta, datetime

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import bid
from ttd.ttdenv import TtdEnvFactory
from ttd.interop.vertica_import_operators import VerticaImportFromCloud, LogTypeFrequency

from ttd.operators.dataset_check_sensor import DatasetCheckSensor

###########################################
#   Job Configs
###########################################
job_start_date = datetime(2025, 6, 18, 22, 0, 0)
job_environment = TtdEnvFactory.get_from_system()

####################################################################################################################
# DAG
####################################################################################################################

aws_region = "us-east-1"
log_uri = "s3://thetradedesk-useast-avails/emr-logs"
core_fleet_capacity = 512

# Most of the errors are transient while spinning a cluster and sometimes upstream
# Retry every 10 minutes up to 3 hours
max_retries: int = 18
retry_delay: timedelta = timedelta(minutes=10)

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id="vertica-export-publisher-avails-pipeline-{0}-hourly".format(aws_region),
    start_date=job_start_date,
    schedule_interval=timedelta(hours=1),
    max_active_runs=5,
    retries=max_retries,
    retry_delay=retry_delay,
    slack_tags=bid.jira_team,
    slack_channel=bid.alarm_channel,
    enable_slack_alert=False
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
)

std_core_instance_types = [
    R6gd.r6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R5d.r5d_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
]

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

standard_cluster_tags = {"Team": bid.jira_team, "Process": "VerticaExportPublisherAggregate-Avails-{0}".format(aws_region)}

cluster_task_name = "VerticaExportPublisherAvailsPipelineHourly-{0}".format(aws_region)

hourly_cluster_task = EmrClusterTask(
    name=cluster_task_name,
    log_uri=log_uri,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags=standard_cluster_tags,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    environment=job_environment,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region
)

####################################################################################################################
# steps
####################################################################################################################

job_jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline.jar"
job_class_name = "com.thetradedesk.availspipeline.spark.jobs.VerticaExportPublisherAggHourlyTransformOperation"
spark_options = [
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
]

eldorado_config_option_pairs_list = [('hourToTransform', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                     ('availsRawS3Region', aws_region), ('useDeltaFormat', 'true')]

use_delta_config_name = "ttd.VerticaExportPublisherAggHourlyDataSet.useDeltaVersion"
eldorado_config_option_pairs_list.append((use_delta_config_name, 'false'))

job_step = EmrJobTask(
    name="VerticaExportPublisherAggHourlyTransform",
    class_name=job_class_name,
    executable_path=job_jar_path,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)

vertica_import_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="VerticaExportPublisherAggHourly_OpenLWGate",
    gating_type_id=2000683,
    log_type_frequency=LogTypeFrequency.HOURLY,
    vertica_import_enabled=True,
    job_environment=job_environment,
)
job_step >> vertica_import_task

hourly_cluster_task.add_parallel_body_task(job_step)

dag >> hourly_cluster_task

# Potentially we could have 1 EmrClusterTask for each source type and release them individually
wait_complete = DatasetCheckSensor(
    dag=dag.airflow_dag,
    ds_date="{{ logical_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    datasets=[AvailsDatasources.publisher_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1")],
    timeout=60 * 60 * 12  # 12 hours to give upstream job to complete
)

# we won't create the cluster until all dependent upstream datasets are created
wait_complete >> hourly_cluster_task.first_airflow_op()

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
adag = dag.airflow_dag
