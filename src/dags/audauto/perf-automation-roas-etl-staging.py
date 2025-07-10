from datetime import datetime, timedelta

from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF

from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.audauto.utils import utils

#################################################################
# Setting Area
#################################################################

# Note: we should not set ttd.experiment=staging, otherwise data source like ConversionTrackerVerticaLoadDataSetV4
# will try to read from s3://ttd-datapipe-data/parquet/test/experiment=staging/...
experiment_name = "staging_roas"
env_specific_setting = ([("jobExperimentName", experiment_name), ('AdGroupPolicyGenerator.SamplingMod', '200')])

# chain data if in prodTest

env_specific_setting = env_specific_setting + utils.get_experiment_setting(experiment_name)

# generic spark settings list we'll add to each step.
num_clusters = 90
num_partitions = int(round(3.1 * num_clusters)) * 10

KONGMING_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/kongming/jars/prod/kongming.jar"

job_setting_list = ([("date", "{{ds}}"), ("task", "roas"), ("conversionLookback", 1), ("AdGroupPolicyGenerator.GlobalDataLookBack", 3),
                     ("AdGroupPolicyGenerator.TryMaintainConfigValue", "false"), ("AdGroupPolicyGenerator.PositivesCountWarmUpDays", 0),
                     ("ttd.env", TtdEnvFactory.get_from_system()), ("clickLookback", 0),
                     ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"), ("redate", "true"),
                     ("OutOfSampleAttributeSetGenerator.ImpressionLookBack", 1),
                     ("OutOfSampleAttributeSetGenerator.AttributionLookBack", 1), ("enablePartitionRegister", "true")] +
                    env_specific_setting + utils.schemaPolicy  # noqa: E121
                    )

##################################################################
# End of Setting Area
#################################################################

#################################################################
# Code Area (Less likely you need to change code area)
#################################################################

spark_options_list = utils.get_spark_options_list(num_partitions)
slack_channel, slack_tags, enable_slack_alert = utils.get_env_vars()

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
roas_etl_dag = TtdDag(
    dag_id="perf-automation-roas-etl-staging",
    start_date=datetime(2024, 12, 1),
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
dag = roas_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
# hour_dataset takes version, environment into consideration in constructing file path.
# However, it concatnates with bucket/path_prefix/env/data_name/version.
# bidimpression data are stored in this format.
# s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=2024/month=08/day=21/hourPart=3/_SUCCESS

geronimo_etl_dataset = utils.get_geronimo_etl_dataset()
conversion_tracker_dataset = utils.get_conversion_tracker_dataset()
attributedevent_dataset = utils.get_attribute_event_dataset()
attributedeventresult_dataset = utils.get_attribute_event_result_dataset()

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor_task = OpTask(
    op=DatasetCheckSensor(
        task_id="data_available",
        datasets=[
            geronimo_etl_dataset,
            conversion_tracker_dataset,
            attributedevent_dataset,
            attributedeventresult_dataset,
        ],
        ds_date="{{logical_date.at(23).to_datetime_string()}}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

# todo: revisit storage configs
roas_etl_cluster = utils.create_emr_cluster("ROASEtlCluster", num_clusters)

# Data ETL steps
daily_adgroup_policy_snapshot = utils.create_emr_spark_job(
    "AdGroupPolicyGenerator", "job.AdGroupPolicyGenerator", KONGMING_JAR, spark_options_list, job_setting_list, roas_etl_cluster
)

daily_bids_impresssions = utils.create_emr_spark_job(
    "DailyBidsImpressionsFilter", "job.DailyBidsImpressions", KONGMING_JAR, spark_options_list, job_setting_list, roas_etl_cluster,
    timedelta(hours=3)
)

daily_score_set = utils.create_emr_spark_job(
    "DailyScoreSet", "job.DailyOfflineScoringSet", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list, roas_etl_cluster, timedelta(hours=1)
)

daily_oos_set = utils.create_emr_spark_job(
    "DailyOutOfSampleSet", "job.OutOfSampleAttributionSetGenerator", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list, roas_etl_cluster, timedelta(hours=1)
)
daily_exchange = utils.create_emr_spark_job(
    "DailyExchange", "job.DailyExchangeRate", KONGMING_JAR, spark_options_list + [("conf", "spark.sql.autoBroadcastJoinThreshold=-1")],
    job_setting_list, roas_etl_cluster, timedelta(hours=1)
)

# Data ETL steps
daily_feedback_snapshot = utils.create_emr_spark_job(
    "DailyFeedbackSignals", "job.DailyFeedbackSignals", KONGMING_JAR, spark_options_list, job_setting_list, roas_etl_cluster,
    timedelta(hours=1)
)

daily_attr_events = utils.create_emr_spark_job(
    "DailyAttributedEvents", "job.DailyAttributedEvents", KONGMING_JAR, spark_options_list, job_setting_list, roas_etl_cluster
)

generate_train_set_task = utils.create_emr_spark_job(
    "GenerateTrainSetRevenueLastTouch", "job.GenerateTrainSetRevenueLastTouch", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ("addBidRequestId", "true"),
    ], roas_etl_cluster, timedelta(hours=6)
)

generate_train_set_click_task = utils.create_emr_spark_job(
    "GenerateTrainSetRevenueClick", "job.GenerateTrainSetClick", KONGMING_JAR, spark_options_list + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
    ], job_setting_list + [
        ("addBidRequestId", "true"),
    ], roas_etl_cluster, timedelta(hours=6)
)

final_dag_check_optask = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# Dag steps
roas_etl_dag >> dataset_sensor_task >> roas_etl_cluster >> final_dag_check_optask

daily_adgroup_policy_snapshot >> daily_bids_impresssions >> daily_score_set >> daily_exchange >> daily_feedback_snapshot >> \
    daily_attr_events >> daily_oos_set >> generate_train_set_task >> generate_train_set_click_task
