from datetime import datetime, timedelta

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c6g import C6g
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import FORECAST

SAMPLING_JOB_CLASS = "com.thetradedesk.etlforecastjobs.preprocessing.sampledavails.PersonSampledAvailsGeneratorJob"
JAR_PATH = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
AVAILS_BUCKET_NAME = "ttd-sampled-avails-useast1"

IAV2_OPEN_GRAPH_NAME = "openGraphIav2"

NUM_CORE_UNITS = 2400
target_date_hour = "{{ dag_run.conf['target-date-hour'] }}"

dag_name = "person-sampled-avails-generation-dag"
job_start_date = datetime(2025, 1, 2, 1)
job_end_date = None

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    end_date=job_end_date,
    schedule_interval=timedelta(hours=1),
    slack_tags=FORECAST.data_charter().sub_team,
    slack_channel=FORECAST.team.alarm_channel,
    tags=[FORECAST.team.jira_team],
    max_active_runs=8,
)

sampling_gen_dag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[C6g.c6g_8xlarge().with_ebs_size_gb(768).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)


def get_core_fleet_instance_type_configs(num_cores: int) -> EmrFleetInstanceTypes:
    return EmrFleetInstanceTypes(
        instance_types=[
            C6g.c6g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(16),
            C6g.c6g_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(32),
            C6g.c6g_12xlarge().with_ebs_size_gb(768).with_fleet_weighted_capacity(48),
            C6g.c6g_16xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(64),
            C7g.c7g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(16),
            C7g.c7g_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(32),
            C7g.c7g_12xlarge().with_ebs_size_gb(768).with_fleet_weighted_capacity(48),
            C7g.c7g_16xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(64),
        ],
        on_demand_weighted_capacity=num_cores,
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
    # ("conf", "spark.databricks.delta.optimizeWrite.enabled=true"), # TODO: add this back when we upgrade to delta 3.1 and spark 3.5

    # Dynamo stuff
    ("conf", "spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore"),
    ("conf", "spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore"),
    ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-east-1"),
    ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=avails_pipeline_delta_log"),
]

# task to wait for input dataset - do this before spinning up EMR cluster
wait_for_input_data = DatasetCheckSensor(
    dag=dag.airflow_dag,
    task_id="wait-for-id-and-deal-agg-data",

    # this will wait for the same hour to be ready as 1 hour before the current execution time so that we don't wait most of the time
    # basically, you will want to pass the same time here as the time you pass to your EMR step that tells your spark
    # job which date/hour partition to read
    ds_date="{{ logical_date.subtract(hours=1).to_datetime_string() }}",

    # How often to poll - an interval of 1 minute seems to break. 10 minutes is more friendly to the scheduler
    poke_interval=60 * 10,

    # Increase from default 2 hours to 16 hours since it frequently times out on source data
    timeout=60 * 60 * 16,

    # Which datasets you want to wait on. You could pass multiple in here if you want
    datasets=[
        AvailsDatasources.identity_and_deal_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1"),
    ]
)

# Keeping this as an array just in case new graphs are required in the future.
graph_names = [IAV2_OPEN_GRAPH_NAME]
cluster_task = EmrClusterTask(
    name="PersonSampledAvailsGeneratorJob",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": FORECAST.team.jira_team},
    core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(NUM_CORE_UNITS),
    # Can't be upgraded until we've figured out the delta DeleteFromTable issues
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2_1,
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations],
)

for graphName in graph_names:
    person_sampled_avail_v2_task = EmrJobTask(
        name=f"PersonSampledAvailsGeneratorJob-{graphName}",
        class_name=SAMPLING_JOB_CLASS,
        executable_path=JAR_PATH,
        eldorado_config_option_pairs_list=[
            ("dateHour", "{{ (logical_date.subtract(hours=1)).strftime(\"%Y-%m-%dT%H:00:00\") }}"),
            ("graphName", graphName),
            ("bucketName", AVAILS_BUCKET_NAME),
            ("repartitionBeforeWriting", "true"),  # TODO: we can turn it off once we turn on optimizeWrite
            ("writePartitions", 1000)
        ],
        cluster_specs=cluster_task.cluster_specs,
        configure_cluster_automatically=False,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
        timeout_timedelta=timedelta(hours=6),
        retries=2
    )
    cluster_task.add_sequential_body_task(person_sampled_avail_v2_task)

dag >> cluster_task
wait_for_input_data >> cluster_task.first_airflow_op()
