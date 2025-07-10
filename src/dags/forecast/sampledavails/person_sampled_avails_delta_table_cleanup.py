from datetime import datetime, timedelta

from dags.forecast.sampledavails.person_sampled_avails_generation import IAV2_OPEN_GRAPH_NAME
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import FORECAST

CLEANUP_JOB_CLASS = "com.thetradedesk.etlforecastjobs.preprocessing.sampledavails.PersonSampledAvailsDeltaTableCleanupJob"
JAR_PATH = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
AVAILS_BUCKET_NAME = "ttd-sampled-avails-useast1"
NUM_CORE_UNITS = 320

job_start_date = datetime(2025, 1, 2, 1, 40)
job_end_date = None

dag = TtdDag(
    dag_id="person-sampled-avails-cleanup-dag",
    start_date=job_start_date,
    end_date=job_end_date,
    schedule_interval=timedelta(days=1),
    slack_tags=FORECAST.data_charter().sub_team,
    slack_channel=FORECAST.team.alarm_channel,
    tags=[FORECAST.team.jira_team],
    max_active_runs=1,
)

cleanup_dag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(1024)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5d.m5d_4xlarge().with_fleet_weighted_capacity(16).with_ebs_size_gb(512),
        M5d.m5d_8xlarge().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
        M6g.m6g_4xlarge().with_fleet_weighted_capacity(16).with_ebs_size_gb(512),
        M6g.m6g_8xlarge().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
    ],
    on_demand_weighted_capacity=NUM_CORE_UNITS,
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

additional_args_option_pairs_list = [
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("conf", "spark.databricks.delta.retentionDurationCheck.enabled=false"),
    ("conf", "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled=true"),
    ("conf", "spark.databricks.delta.vacuum.parallelDelete.enabled=true"),
]

additional_dynamo_list = [
    # Dynamo stuff
    ("conf", "spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore"),
    ("conf", "spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore"),
    ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-east-1"),
    ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=avails_pipeline_delta_log"),
]

cleanup_cluster_task = EmrClusterTask(
    name="PersonSampledAvailsDeltaTableCleanupJob",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": FORECAST.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    # Can't be upgraded until we've figured out the delta DeleteFromTable issues
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2_1,
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations],
)

# Keeping this as an array just in case new graphs are required in the future.
graphs = [IAV2_OPEN_GRAPH_NAME]
# Define parameters and combinations
for graph in graphs:
    delete_task = EmrJobTask(
        name=f"PersonSampledAvailsDeltaTableCleanupJob-{graph}-delete-task",
        class_name=CLEANUP_JOB_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[
            ("runDate", "{{ (logical_date).strftime(\"%Y-%m-%d\") }}"),
            ("bucketName", AVAILS_BUCKET_NAME),
            ("deltaOperation", "delete"),
            ("graphName", graph),
            ("vacuumRetentionHours", 24),
        ],
        cluster_specs=cleanup_cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list + additional_dynamo_list,
    )

    vacuum_task = EmrJobTask(
        name=f"PersonSampledAvailsDeltaTableCleanupJob-{graph}-vacuum-task",
        class_name=CLEANUP_JOB_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[
            ("runDate", "{{ (logical_date).strftime(\"%Y-%m-%d\") }}"),
            ("bucketName", AVAILS_BUCKET_NAME),
            ("deltaOperation", "vacuum"),
            ("graphName", graph),
            ("vacuumRetentionHours", 24),
        ],
        cluster_specs=cleanup_cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )

    cleanup_cluster_task.add_sequential_body_task(delete_task)
    cleanup_cluster_task.add_sequential_body_task(vacuum_task)

dag >> cleanup_cluster_task
