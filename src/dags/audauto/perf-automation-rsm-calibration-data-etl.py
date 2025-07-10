import copy
from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.confetti.auto_configured_emr_job_task import AutoConfiguredEmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [("executor-memory", "203G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=128G"),
                      ("conf", "spark.driver.cores=20"), ("conf", "spark.sql.shuffle.partitions=8192"),
                      ("conf", "spark.default.parallelism=8192"), ("conf", "spark.driver.maxResultSize=80G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

run_date = "{{ data_interval_start.to_date_string() }}"
date_str = "{{ data_interval_start.strftime(\"%Y%m%d000000\") }}"
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3_2
environment = TtdEnvFactory.get_from_system()
env = environment.execution_env

experiment = ""
experiment_suffix = f"/experiment={experiment}" if experiment else ""
override_env = f"test{experiment_suffix}" if env == "prodTest" else env  # only apply experiment suffix in prodTest
oos_read_env = override_env

PROD_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
TEST_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
AUDIENCE_JAR = PROD_JAR if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else TEST_JAR

calibration_data_etl_dag = TtdDag(
    dag_id="perf-automation-rsm-v2-calibration-data-etl",
    start_date=datetime(2025, 5, 25, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "RSMV2"]
)

dag = calibration_data_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
seed_none_oos_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"data/{oos_read_env}",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_OOS_SUCCESS"
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[seed_none_oos_etl_success_file],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

###############################################################################
# clusters
###############################################################################
audience_calibration_data_etl_cluster_task = EmrClusterTask(
    name="AudienceCalibrationDataGenerationCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32)],
        on_demand_weighted_capacity=3840
    ),
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300
)

###############################################################################
# steps
###############################################################################
audience_rsm_calibration_data_generation_step = AutoConfiguredEmrJobTask(
    group_name="audience",
    job_name="CalibrationDataGenerator",
    name="CalibrationDataGenerator",
    class_name="com.thetradedesk.audience.jobs.CalibrationInputDataGeneratorJob",
    emr_job_kwargs=dict(
        additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
            ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
        ],
        eldorado_config_option_pairs_list=[
            ('date', run_date),
            ('lookBack', '10'),
            ('startDate', '2025-03-20'),
            ('ttdReadEnv', oos_read_env),  # calibration reads only from OOS
            ('ttdWriteEnv', override_env)
        ],
        executable_path=AUDIENCE_JAR,
        timeout_timedelta=timedelta(hours=4)
    )
)

write_etl_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_oos_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_CALIBRATION_SUCCESS",
        date="",
        append_file=False,
        dag=dag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

audience_calibration_data_etl_cluster_task.add_parallel_body_task(audience_rsm_calibration_data_generation_step)

calibration_data_etl_dag >> dataset_sensor >> audience_calibration_data_etl_cluster_task >> write_etl_success_file_task >> final_dag_status_step
