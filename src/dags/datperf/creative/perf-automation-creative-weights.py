from datetime import datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from dags.datperf.datasets import platformreport_dataset

dag_name = "perf-automation-creative-weights"

exec_date = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
datperf_jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar"

# Environment
env = TtdEnvFactory.get_from_system()

###############################################################################
# DAG
###############################################################################

creative_weights_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 12, 3),
    schedule_interval="0 1 * * *",
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/vNoMCQ',
    retries=1,
    max_active_runs=1,
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = creative_weights_dag.airflow_dag

###############################################################################
# S3 dataset sensors
###############################################################################

# S3 sensors
platform_report_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='platform_report_data_available',
        datasets=[platformreport_dataset],
        # looks for success file in hour 23
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

###############################################################################
# EMR clusters
###############################################################################

# EMR version to run
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5_0

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5d.m5d_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5d.m5d_4xlarge().with_ebs_size_gb(128).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=12
)

spark_options_list = [
    ("executor-memory", "45G"),
    ("executor-cores", "16"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=45G"),
    ("conf", "spark.driver.core=16"),
    ("conf", "spark.sql.shuffle.partitions=300"),
    ("conf", "spark.driver.maxResultSize=10G"),
    # ("conf", "spark.yarn.maxAppAttempts=1"),
    # ("conf", "spark.yarn.executor.memoryOverhead=45G"),
    # ("conf", "spark.default.parallelism=6000"),
]

cluster = EmrClusterTask(
    name="creative-weights-job",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": DATPERF.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=emr_release_label,
)

creative_weights_job = EmrJobTask(
    name="optimizeCreatives",
    class_name="com.thetradedesk.jobs.creativeweights.OptimizeCreatives",
    executable_path=datperf_jar,
    eldorado_config_option_pairs_list=[
        ("ttd.env", env),
        ("date", exec_date),
    ],
    additional_args_option_pairs_list=spark_options_list,
)

creative_bandits_weights_job = EmrJobTask(
    name="OptimizeCreativeBandits",
    class_name="com.thetradedesk.jobs.creativeweights.OptimizeCreativeBandits",
    executable_path=datperf_jar,
    eldorado_config_option_pairs_list=[
        ("ttd.env", env),
        ("date", exec_date),
    ],
    additional_args_option_pairs_list=spark_options_list,
)

merge_bandits_weights_job = EmrJobTask(
    name="MergeBanditWeights",
    class_name="com.thetradedesk.jobs.creativeweights.MergeBanditWeights",
    executable_path=datperf_jar,
    eldorado_config_option_pairs_list=[
        ("ttd.env", env),
        ("date", exec_date),
        ("epsilon", 0.25),
    ],
    additional_args_option_pairs_list=spark_options_list,
)

creative_weights_job >> creative_bandits_weights_job
creative_weights_job >> merge_bandits_weights_job
creative_bandits_weights_job >> merge_bandits_weights_job

cluster.add_parallel_body_task(creative_weights_job)
cluster.add_parallel_body_task(creative_bandits_weights_job)
cluster.add_parallel_body_task(merge_bandits_weights_job)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
creative_weights_dag >> platform_report_sensor >> cluster
cluster.last_airflow_op() >> final_dag_status_step
