# This import is necesarry for Airflow to see the file and run it, even though we've offloaded all the actual airflow calls to a sub library
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5b import R5b
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from datetime import datetime, timedelta

from ttd.slack.slack_groups import AUDAUTO

executionIntervalInDays = 1

# EMR version to run
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2


# parameters
def execution_date(format):
    # we use replace because format causes issues with {}
    return '{{ logical_date.add(days=$interval$).strftime("$format$") }}' \
        .replace("$interval$", str(executionIntervalInDays)).replace("$format$", format)


# generic settings list we'll add to each step
java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"), ('date', execution_date("%Y-%m-%d"))]

# generic spark settings list we'll add to each step.
spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC")]

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-audauto-assembly.jar"

# The top-level dag
fpcs_dag = TtdDag(
    dag_id="perf-automation-fpcs",
    start_date=datetime(2025, 5, 20, 15, 0),
    schedule_interval=timedelta(days=executionIntervalInDays),
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=False
)
dag = fpcs_dag.airflow_dag

# === Cluster ===
fpcs_cluster = EmrClusterTask(
    name="FirstPartyCookieScoring",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5a.r5a_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5b.r5b_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5a.r5a_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(64)
        ],
        on_demand_weighted_capacity=8640
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=emr_release_label
)

fpcs_perf_calc_cluster = EmrClusterTask(
    name="FirstPartyCookieScoringPerfCalculation",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5a.r5a_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5a.r5a_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5a.r5a_12xlarge().with_ebs_size_gb(768).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=3840
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=emr_release_label
)

campaign_scores_step = EmrJobTask(
    name="GenerateCampaignScores",
    class_name="model.fpcookiescoring.GenerateCampaignScores",
    timeout_timedelta=timedelta(hours=8),
    additional_args_option_pairs_list=spark_options_list + [("conf", "spark.executor.memory=180g"), ("conf", "spark.driver.memory=180g"),
                                                            ("conf", "spark.driver.cores=30"), ("conf", "spark.driver.maxResultSize=0"),
                                                            ("conf", "spark.driver.memoryOverhead=56g"),
                                                            ("conf", "spark.executor.cores=30"),
                                                            ("conf", "spark.executor.memoryOverhead=56g"),
                                                            ("conf", "spark.sql.shuffle.partitions=12000"),
                                                            ("conf", "spark.dynamicAllocation.enabled=true"),
                                                            ("conf", "spark.network.timeout=14400s"),
                                                            ("conf", "spark.executor.heartbeatInterval=600s")],
    eldorado_config_option_pairs_list=java_settings_list + [('useNewSources', 'true'), ('trainingWindowDays', '14'),
                                                            ('labelWindowDays', '1'), ('testWindowDays', '2'),
                                                            ('openlineage.enable', 'false')],
    executable_path=jar_path
)

fpcs_model_step = EmrJobTask(
    name="FirstPartyCookieScoringModel",
    class_name="model.fpcookiescoring.FirstPartyCookieScoringModel",
    additional_args_option_pairs_list=spark_options_list + [("conf", "spark.executor.memory=180g"), ("conf", "spark.driver.memory=180g"),
                                                            ("conf", "spark.driver.cores=30"), ("conf", "spark.driver.maxResultSize=0"),
                                                            ("conf", "spark.driver.memoryOverhead=56g"),
                                                            ("conf", "spark.executor.cores=30"),
                                                            ("conf", "spark.executor.memoryOverhead=56g"),
                                                            ("conf", "spark.sql.shuffle.partitions=6000"),
                                                            ("conf", "spark.dynamicAllocation.enabled=true")],
    eldorado_config_option_pairs_list=java_settings_list + [('useNewSources', 'true'), ('trainingWindowDays', '14'),
                                                            ('labelWindowDays', '1'), ('testWindowDays', '2'),
                                                            ('openlineage.enable', 'false')],
    executable_path=jar_path
)

cs_to_advertiser_data_step = EmrJobTask(
    name="CookieScoringToAdvertiserData",
    class_name="model.fpcookiescoring.CookieScoringToAdvertiserData",
    additional_args_option_pairs_list=spark_options_list + [("conf", "spark.driver.memory=40g")],
    eldorado_config_option_pairs_list=java_settings_list + [('usersPerFile', '20000'), ('openlineage.enable', 'false')],
    executable_path=jar_path
)

fpcs_perf_step = EmrJobTask(
    name="FpcsPerformance",
    class_name="model.fpcookiescoring.FpcsPerformance",
    additional_args_option_pairs_list=spark_options_list + [("conf", "spark.executor.memory=100g"), ("conf", "spark.driver.memory=100g"),
                                                            ("conf", "spark.driver.cores=15"), ("conf", "spark.driver.maxResultSize=0"),
                                                            ("conf", "spark.driver.memoryOverhead=18g"),
                                                            ("conf", "spark.executor.cores=15"),
                                                            ("conf", "spark.executor.memoryOverhead=18g"),
                                                            ("conf", "spark.sql.shuffle.partitions=18000"),
                                                            ("conf", "spark.dynamicAllocation.enabled=true")],
    eldorado_config_option_pairs_list=java_settings_list + [('LookbackWindowDays', '3'), ('openlineage.enable', 'false')],
    executable_path=jar_path
)

fpcs_cluster.add_sequential_body_task(campaign_scores_step)
fpcs_cluster.add_sequential_body_task(fpcs_model_step)
fpcs_cluster.add_sequential_body_task(cs_to_advertiser_data_step)

fpcs_perf_calc_cluster.add_sequential_body_task(fpcs_perf_step)

# setup step dependencies for the model
fpcs_dag >> fpcs_cluster >> fpcs_perf_calc_cluster

final_dag_check = FinalDagStatusCheckOperator(dag=dag)
fpcs_perf_calc_cluster.last_airflow_op() >> final_dag_check
