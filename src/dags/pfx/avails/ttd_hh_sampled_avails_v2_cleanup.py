# Cleanup job V2 Household sampling avails
# This triggers scala job that will delete expired partitions and also do delta vacuum operation
from datetime import datetime, timedelta

from dags.tv.constants import FORECAST_JAR_PATH
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack import slack_groups
from ttd.slack.slack_groups import PFX

CLEANUP_JOB_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.avails.DeltaTableCleanupJob"
JAR_PATH = FORECAST_JAR_PATH  # JAR_PATH = "s3://ttd-ctv/test/shawn.tang/lib/etl-forecast-jobs.jar"
AVAILS_BUCKET_NAME = "ttd-sampled-avails-useast1"
NUM_CORE_UNITS = 320

job_start_date = datetime(2024, 10, 30, 1, 40)
job_end_date = None

dag = TtdDag(
    dag_id="ttd-hh-sampling-avails-v2-cleanup-dag",
    start_date=job_start_date,
    end_date=job_end_date,
    schedule_interval=timedelta(days=1),
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv"],
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
    name="ttd-hh-sample-cleanup",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.PFX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2_1,
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations],
)

# Define parameters and combinations
graphs = ["iav2", "adBrain", "openGraphIav2", "openGraphAdBrain"]
sample_types = ["lowSample", "highSample"]
for graph in graphs:
    for sample in sample_types:
        delete_task = EmrJobTask(
            name=f"{graph}-{sample}-delete-task",
            class_name=CLEANUP_JOB_CLASS,
            executable_path=JAR_PATH,
            eldorado_config_option_pairs_list=[
                ("runDate", "{{ (logical_date).strftime(\"%Y-%m-%d\") }}"),
                ("bucketName", AVAILS_BUCKET_NAME),
                ("deltaOperation", "delete"),
                ("graph", graph),
                ("isLowSample", "true" if sample == "lowSample" else "false"),
                ("vacuumRetentionHours", 24),
            ],
            cluster_specs=cleanup_cluster_task.cluster_specs,
            configure_cluster_automatically=False,
            additional_args_option_pairs_list=additional_args_option_pairs_list + additional_dynamo_list,
        )

        vacuum_task = EmrJobTask(
            name=f"{graph}-{sample}-vacuum-task",
            class_name=CLEANUP_JOB_CLASS,
            executable_path=JAR_PATH,
            eldorado_config_option_pairs_list=[
                ("runDate", "{{ (logical_date).strftime(\"%Y-%m-%d\") }}"),
                ("bucketName", AVAILS_BUCKET_NAME),
                ("deltaOperation", "vacuum"),
                ("graph", graph),
                ("isLowSample", "true" if sample == "lowSample" else "false"),
                ("vacuumRetentionHours", 24),
            ],
            cluster_specs=cleanup_cluster_task.cluster_specs,
            configure_cluster_automatically=False,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
        )

        cleanup_cluster_task.add_sequential_body_task(delete_task)
        cleanup_cluster_task.add_sequential_body_task(vacuum_task)

dag >> cleanup_cluster_task
