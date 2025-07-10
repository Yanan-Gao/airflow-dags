from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask, EmrClusterTask
from ttd.ec2.emr_instance_types.memory_optimized.r6id import R6id
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource

from ttd.ttdenv import TtdEnvFactory

dag_name = "perf-automation-vcr"

DATE = '{{ data_interval_start.strftime("%Y%m%d") }}'

docker_registry = "production.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/vcrprediction"
docker_image_tag = "latest"

CLUSTERSETUP = "s3://thetradedesk-mlplatform-us-east-1/libs/vcr/scripts/clustersetup.sh"
MODELRUN = "s3://thetradedesk-mlplatform-us-east-1/libs/vcr/scripts/modelrun.sh"

ENV = TtdEnvFactory.get_from_system()

if ENV.execution_env.lower() == "prodtest":
    CLUSTERSETUP = f"s3://thetradedesk-mlplatform-us-east-1/libs/vcr/scripts/mergerequests/{docker_image_tag}/clustersetup.sh"
    MODELRUN = f"s3://thetradedesk-mlplatform-us-east-1/libs/vcr/scripts/mergerequests/{docker_image_tag}/modelrun.sh"

MODELVERSION = '3'  # string for shell script this will run model 3 and 4
EPOCHS = '10'
SAMPLE_SIZE = 0.075
LOOKBACK_DAYS = 7
####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
vcr_dag = TtdDag(
    dag_id=dag_name,
    # want job start date to be 1 am UTC, run everyday
    start_date=datetime(2024, 11, 1),
    schedule_interval='0 1 * * *',
    dag_tsg='',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF', "vcr"],
    enable_slack_alert=False,
)

dag = vcr_dag.airflow_dag

####################################################################################################################
# S3 key sensors
####################################################################################################################

video_event_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='video_data_available',
        datasets=[RtbDatalakeDatasource.rtb_videoevent_v5],
        # looks for success file in hour 23
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

bidrequest_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='bidrequest_data_available',
        datasets=[RtbDatalakeDatasource.rtb_bidrequest_v5],
        # looks for success file in hour 23
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

####################################################################################################################
# clusters
####################################################################################################################
master_fleet_instance_type_configs_etl = EmrFleetInstanceTypes(
    instance_types=[R6id.r6id_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs_etl = EmrFleetInstanceTypes(
    instance_types=[R6id.r6id_8xlarge().with_fleet_weighted_capacity(16)],
    on_demand_weighted_capacity=320,
)

etl_cluster = DockerEmrClusterTask(
    name=dag_name + "_etl_" + ENV.execution_env,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs_etl,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs_etl,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri="s3://ttd-identity/datapipeline/logs/airflow",
)

model_train_cluster = EmrClusterTask(
    name=dag_name + "_model_" + ENV.execution_env,
    # only a master node for this training, but cant define a single node cluster with runjobflow operator
    # would usually just use an ec2 instance but ongoing battle with secops et. al. has left us unable to provision
    # from ec2 at the moment so picking single cheapest instance for this cluster for now
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_16xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            M5.m5_xlarge().with_fleet_weighted_capacity(1),
            M5.m5_2xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1
    ),
    emr_release_label='emr-6.9.0',
    cluster_auto_termination_idle_timeout_seconds=-1
)

cluster_setup = EmrJobTask(
    name="ClusterSetup",
    job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
    executable_path=CLUSTERSETUP,
    class_name=None
)
####################################################################################################################
# steps
####################################################################################################################
additional_spark_options_list_etl = [("executor-memory", "5G"), ("executor-cores", "1"),
                                     ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                     ("conf", "spark.driver.memory=210G"), ("conf", "spark.driver.cores=32"),
                                     ("conf", "spark.sql.shuffle.partitions=5000"), ("conf", "spark.default.parallelism=5000"),
                                     ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.executor.memoryOverhead=1G"),
                                     ("conf", "spark.dynamicAllocation.enabled=true"),
                                     ("conf", "spark.sql.adaptive.coalescePartitions.minPartitionNum=3040"),
                                     ("conf", "spark.sql.adaptive.coalescePartitions.enabled=false")]

etl_task = PySparkEmrTask(
    name="etl_task",
    entry_point_path="/home/hadoop/app/mainetl.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=additional_spark_options_list_etl,
    command_line_arguments=[
        f"--env={ENV}",
        f"--date={DATE}",
        f"--sample_size={SAMPLE_SIZE}",
        f"--lookback_days={LOOKBACK_DAYS}",
    ],
    timeout_timedelta=timedelta(hours=18),
)

etl_cluster.add_parallel_body_task(etl_task)

training_args = ["-v", MODELVERSION, "-d", DATE, "-e", ENV.execution_env.lower(), "-n", EPOCHS, "-t", docker_image_tag]

model_task = EmrJobTask(
    name="model_task",
    job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
    executable_path=MODELRUN,
    command_line_arguments=training_args,
    timeout_timedelta=timedelta(hours=18),
    class_name=None
)

model_train_cluster.add_sequential_body_task(cluster_setup)
model_train_cluster.add_sequential_body_task(model_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG dependencies

vcr_dag >> video_event_dataset_sensor >> bidrequest_dataset_sensor >> etl_cluster
etl_cluster >> model_train_cluster >> final_dag_status_step
