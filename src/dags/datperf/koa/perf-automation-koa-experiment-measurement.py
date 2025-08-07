from datasources.sources.common_datasources import CommonDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from datetime import datetime

from ttd.tasks.op import OpTask

RUNTIME_DATE = '{{ (data_interval_end).strftime("%Y-%m-%dT%H:00:00") }}'
JOB_DATE = '{{ (data_interval_start).strftime("%Y-%m-%d") }}'
KOA_EXPERIMENT_JAR = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar"

####################################################################################################################
# DAG
####################################################################################################################

koa_experiment_dag = TtdDag(
    dag_id="perf-automation-koa-experiment-measurement",
    start_date=datetime(2024, 8, 19),
    schedule_interval="0 6 * * *",
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/vNoMCQ',
    retries=1,
    max_active_runs=1,
    tags=['DATPERF'],
    enable_slack_alert=False,
)

dag = koa_experiment_dag.airflow_dag

####################################################################################################################
# S3 dataset sources
####################################################################################################################

# RTB Platform Reports
platform_report_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

# AdGroupPerformance Data
adgroup_performance_dataset = DateGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    data_name="models/autoopt/adgroupperformanceimprovement",
    version=1,
    env_aware=True,
    success_file=None
)

####################################################################################################################
# S3 dataset sensors
####################################################################################################################

# S3 sensors
platform_report_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='platform_report_data_available',
        datasets=[platform_report_dataset],
        # looks for success file in hour 23
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

bidfeedback_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='bidfeedback_data_available',
        datasets=[CommonDatasources.rtb_bidfeedback_v5],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

adgroup_performance_improvement_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='adgroup_performance_improvement_available',
        datasets=[adgroup_performance_dataset],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

####################################################################################################################
# clusters
####################################################################################################################

# EMR version to run
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(32),
        R5.r5_12xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R5.r5_16xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5.r5_24xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(96)
    ],
    on_demand_weighted_capacity=800
)

spark_options_list = [("executor-memory", "202G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=200G"),
                      ("conf", "spark.driver.core=8"), ("conf", "spark.sql.shuffle.partitions=6000"),
                      ("conf", "spark.driver.maxResultSize=50G"), ("conf", "spark.yarn.maxAppAttempts=1"),
                      ("conf", "spark.yarn.executor.memoryOverhead=45G"), ("conf", "spark.default.parallelism=6000")]

cluster = EmrClusterTask(
    name="koa-experiment-measurement-job",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=emr_release_label
)

koa_experiment_reportAgg = EmrJobTask(
    name="koaExperimentReportAgg",
    class_name="com.thetradedesk.jobs.koaexperiment.RTBReportCumulativeAgg",
    executable_path=KOA_EXPERIMENT_JAR,
    additional_args_option_pairs_list=spark_options_list,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[("runTime", RUNTIME_DATE), ("date", JOB_DATE)]
)

cluster.add_parallel_body_task(koa_experiment_reportAgg)

koa_v3_experiment_measurement = EmrJobTask(
    name="koaV3ExperimentMeasurement",
    class_name="com.thetradedesk.jobs.koaexperiment.KoaV3Experiment",
    executable_path=KOA_EXPERIMENT_JAR,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[("runTime", RUNTIME_DATE), ("date", JOB_DATE)]
)

cluster.add_parallel_body_task(koa_v3_experiment_measurement)

koa_sd_experiment_measurement = EmrJobTask(
    name="KoaSdExperimentMeasurement",
    class_name="com.thetradedesk.jobs.koaexperiment.KoaSdExperiment",
    additional_args_option_pairs_list=spark_options_list,
    executable_path=KOA_EXPERIMENT_JAR,
    eldorado_config_option_pairs_list=[("runTime", RUNTIME_DATE), ("date", JOB_DATE)]
)

cluster.add_parallel_body_task(koa_sd_experiment_measurement)

performance_job = EmrJobTask(
    name="PerformanceJob",
    class_name="com.thetradedesk.jobs.koaexperiment.PerformanceJob",
    additional_args_option_pairs_list=spark_options_list,
    executable_path=KOA_EXPERIMENT_JAR,
    eldorado_config_option_pairs_list=[("runTime", RUNTIME_DATE), ("date", JOB_DATE)]
)

cluster.add_parallel_body_task(performance_job)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
koa_experiment_dag >> platform_report_sensor >> cluster
koa_experiment_dag >> bidfeedback_sensor >> cluster
koa_experiment_dag >> adgroup_performance_improvement_sensor >> cluster
koa_experiment_reportAgg >> koa_v3_experiment_measurement >> performance_job
koa_experiment_reportAgg >> koa_sd_experiment_measurement >> performance_job
cluster.last_airflow_op() >> final_dag_status_step
