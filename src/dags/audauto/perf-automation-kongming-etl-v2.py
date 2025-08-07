from datetime import datetime, timedelta
from ttd.eldorado.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

from datasources.sources.common_datasources import CommonDatasources
from datasources.datasources import Datasources
from ttd.slack.slack_groups import DATPERF

from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
from dags.audauto.utils import utils

#################################################################
# Setting Area
#################################################################

# not used for prod, but is used in staging and research
env_specific_setting = ([("dumy", "dumy")])

job_setting_list = ([("date", "{{ds}}"), ("task", "cpa"), ("conversionLookback", 15), ("AdGroupPolicyGenerator.GlobalDataLookBack", 30),
                     ("AdGroupPolicyGenerator.TryMaintainConfigValue", "false"), ("AdGroupPolicyGenerator.PositivesCountWarmUpDays", 0),
                     ("ttd.env", TtdEnvFactory.get_from_system()), ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"),
                     ("enablePartitionRegister", "true")] + env_specific_setting + utils.schemaPolicy)

# generic spark settings list we'll add to each step.
num_clusters = 120
core_node_ebs_per_x = 64

num_partitions = int(round(3.1 * num_clusters)) * 10
# Jar
KONGMING_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/kongming/jars/prod/kongming_production.jar"

calibration_oos_lookback = (12, 2)  # imp, conv lookback to generate labeled calibration data

# OOS Configs for Main ETL (impression lookback, conversion lookback)
oos_lookback_timely = (1, 2)
oos_lookback_groundtruth = (3, 7)
#################################################################
# End of Setting Area
#################################################################

#################################################################
# Code Area (Less likely you need to change code area)
#################################################################

spark_options_list = utils.get_spark_options_list(num_partitions)
slack_channel, slack_tags, enable_slack_alert = utils.get_env_vars()

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
kongming_etl_dag = TtdDag(
    dag_id="perf-automation-kongming-etl-v2",
    start_date=datetime(2025, 7, 1),
    schedule_interval=utils.schedule_interval,
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    retries=2,
    max_active_runs=1,
    depends_on_past=True,
    retry_delay=timedelta(minutes=5),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    run_only_latest=False,
    teams_allowed_to_access=[DATPERF.team.jira_team]
)
dag = kongming_etl_dag.airflow_dag

geronimo_etl_dataset = utils.get_geronimo_etl_dataset()
conversion_tracker_dataset = utils.get_conversion_tracker_dataset()
attribute_event_dataset = utils.get_attribute_event_dataset()
attribute_event_result_dataset = utils.get_attribute_event_result_dataset()

dataset_sensor_task = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[
            CommonDatasources.rtb_bidfeedback_v5, Datasources.rtb_datalake.rtb_clicktracker_v5, conversion_tracker_dataset,
            geronimo_etl_dataset, attribute_event_dataset, attribute_event_result_dataset
        ],
        ds_date='{{logical_date.at(23).to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

update_xcom_incremental_train_task = utils.get_update_xcom_incremental_train_task()

kongming_etl_cluster_large = utils.create_emr_cluster("KongmingEtlClusterLarge", num_clusters * 4)
kongming_etl_cluster = utils.create_emr_cluster("KongmingEtlCluster", num_clusters, core_ebs_per_x=core_node_ebs_per_x)
kongming_etl_cluster_trainset = utils.create_emr_cluster("KongmingETLClusterTrainset", num_clusters * 4, core_ebs_per_x=core_node_ebs_per_x)
kongming_etl_cluster_calibration = utils.create_emr_cluster(
    "KongmingETLClusterCalibration", num_clusters * 4, core_ebs_per_x=core_node_ebs_per_x
)
kongming_etl_cluster_oos = utils.create_emr_cluster("KongmingETLClusterOOS", int(num_clusters * 1.5), core_ebs_per_x=core_node_ebs_per_x)

daily_adgroup_policy_snapshot = utils.create_emr_spark_job(
    "AdGroupPolicyGenerator", "job.AdGroupPolicyGenerator", KONGMING_JAR, spark_options_list, job_setting_list, kongming_etl_cluster_large
)

daily_bids_impresssions = utils.create_emr_spark_job(
    "DailyBidsImpressionsFilter", "job.DailyBidsImpressions", KONGMING_JAR, spark_options_list, job_setting_list,
    kongming_etl_cluster_large, timedelta(hours=8)
)

daily_conversion = utils.create_emr_spark_job(
    "DailyConversion", "job.ConversionDataDailyProcessor", KONGMING_JAR, spark_options_list, job_setting_list, kongming_etl_cluster,
    timedelta(hours=1)
)

daily_feedback_snapshot = utils.create_emr_spark_job(
    "DailyFeedbackSignals", "job.DailyFeedbackSignals", KONGMING_JAR, spark_options_list, job_setting_list, kongming_etl_cluster,
    timedelta(hours=1)
)

daily_attr_events = utils.create_emr_spark_job(
    "DailyAttributedEvents", "job.DailyAttributedEvents", KONGMING_JAR, spark_options_list, job_setting_list + [
        ("OutOfSampleAttributeSetGenerator.AttributionLookBack", "15"),
    ], kongming_etl_cluster
)

daily_score_set = utils.create_emr_spark_job(
    "DailyScoreSet", "job.DailyOfflineScoringSet", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list, kongming_etl_cluster, timedelta(hours=2)
)

generate_train_set_task = utils.create_emr_spark_job(
    "GenerateTrainSetLastTouch", "job.GenerateTrainSetLastTouch", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ("addBidRequestId", "true"),
        ("incTrain", utils.get_incremental_train_template_scala()),
    ], kongming_etl_cluster_trainset, timedelta(hours=6)
)

generate_train_set_click_task = utils.create_emr_spark_job(
    "GenerateTrainSetClick", "job.GenerateTrainSetClick", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ("addBidRequestId", "true"),
        ("incTrain", utils.get_incremental_train_template_scala()),
    ], kongming_etl_cluster_trainset, timedelta(hours=6)
)

generate_calibration_oos_data = utils.create_emr_spark_job(
    "OOSCalibration", "job.OutOfSampleAttributionSetGenerator", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ('OutOfSampleAttributeSetGenerator.ImpressionLookBack', calibration_oos_lookback[0]),
        ('OutOfSampleAttributeSetGenerator.AttributionLookBack', calibration_oos_lookback[1]),
        ('isExactAttributionLookBack', 'false'),
        ('isSampledNegativeWeight', 'true'),
        ("saveTrainingDataAsCBuffer", "false"),
    ], kongming_etl_cluster_calibration, timedelta(hours=2)
)

generate_calibration_sample_data = utils.create_emr_spark_job(
    "GenerateCalibrationData", "job.GenerateCalibrationData", KONGMING_JAR, spark_options_list, job_setting_list + [
        ('GenerateCalibrationData.CalibrationImpLookBack', calibration_oos_lookback[0]),
        ('GenerateCalibrationData.CalibrationAttLookBack', calibration_oos_lookback[1]),
    ], kongming_etl_cluster_calibration, timedelta(hours=2)
)

# OOS Tasks
oos_attribute_tly_task = utils.create_emr_spark_job(
    "OutOfSampleAttributionSetGeneratorTimely", "job.OutOfSampleAttributionSetGenerator", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ("OutOfSampleAttributeSetGenerator.ImpressionLookBack", oos_lookback_timely[0]),
        ("OutOfSampleAttributeSetGenerator.AttributionLookBack", oos_lookback_timely[1]),
        ("OOSPartitionCountFactor", 2),
    ], kongming_etl_cluster_oos
)

oos_attribute_gt_task = utils.create_emr_spark_job(
    "OutOfSampleAttributionSetGeneratorGroundTruth", "job.OutOfSampleAttributionSetGenerator", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ("OutOfSampleAttributeSetGenerator.ImpressionLookBack", oos_lookback_groundtruth[0]),
        ("OutOfSampleAttributeSetGenerator.AttributionLookBack", oos_lookback_groundtruth[1]),
        ("OOSPartitionCountFactor", 2),
    ], kongming_etl_cluster_oos
)

# Dag steps
kongming_etl_dag >> dataset_sensor_task >> kongming_etl_cluster_large >> kongming_etl_cluster >> kongming_etl_cluster_trainset
update_xcom_incremental_train_task >> kongming_etl_cluster_trainset.first_airflow_op()
kongming_etl_cluster >> kongming_etl_cluster_calibration
kongming_etl_cluster >> kongming_etl_cluster_oos

daily_adgroup_policy_snapshot >> daily_bids_impresssions
daily_conversion >> daily_feedback_snapshot >> daily_attr_events >> daily_score_set
daily_score_set >> generate_train_set_click_task >> generate_train_set_task
daily_score_set >> generate_calibration_oos_data >> generate_calibration_sample_data
daily_score_set >> oos_attribute_tly_task >> oos_attribute_gt_task

final_dag_check = FinalDagStatusCheckOperator(dag=dag)
kongming_etl_cluster_trainset.last_airflow_op() >> final_dag_check
kongming_etl_cluster_calibration.last_airflow_op() >> final_dag_check
kongming_etl_cluster_oos.last_airflow_op() >> final_dag_check
