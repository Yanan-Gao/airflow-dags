from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DATPERF, AUDAUTO
from ttd.tasks.op import OpTask
from dags.datperf.utils.spark_config_utils import get_spark_args
from dags.datperf.utils.lwdb_utils import create_fn_lwdb_gate_open
from dags.datperf.datasets import campaignthrottlemetric_dataset, rtbplatformreport_dataset, plutus_dataset

import copy

from ttd.ttdenv import TtdEnvFactory

# Instance configuration
instance_type = R5.r5_8xlarge()
base_ebs_size = 1024
on_demand_weighted_capacity = 20

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)
spark_args = get_spark_args(cluster_params) + \
             [("conf", "spark.memory.fraction=0.7"),  # Increases memory available to spark
              ("conf", "spark.memory.storageFraction=0.25")]

# Jar
PLUTUS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar"

# Testing config
TEST_BRANCH = None

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest and TEST_BRANCH is not None:
    PLUTUS_JAR = f"s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/mergerequests/{TEST_BRANCH}/latest/plutus.jar"

# Ran into error: org.apache.spark.SparkUpgradeException: Error due to upgrading to Spark 3.0: Dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z from Parquet INT96 files may produce different results
# Implemented fix described in error message: You can set the SQL config 'spark.sql.parquet.int96RebaseModeInRead' or the datasource option 'int96RebaseMode' to 'LEGACY' to rebase the datetime values w.r.t. the calendar difference during reading.
additional_conf = [("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]
spark_args.extend(additional_conf)

fileCount = on_demand_weighted_capacity

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
plutus_campaign_backoff_etl = TtdDag(
    dag_id="perf-automation-plutus-campaign-backoff-etl",
    start_date=datetime(2024, 8, 27),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/BAjWDw',
    # todo: Needs a new TSG
    run_only_latest=True,
    retries=0,
    tags=['DATPERF', "Plutus"],
    enable_slack_alert=False,
    default_args={"owner": "DATPERF"},
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutus_campaign_backoff_etl.airflow_dag

daily_dataset_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='daily_dataset_sensor',
    poke_interval=60 * 30,
    timeout=60 * 60 * 12,
    ds_date="{{ data_interval_start.to_datetime_string() }}",
    datasets=[campaignthrottlemetric_dataset, rtbplatformreport_dataset]
)

plutus_dataset_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='plutus_dataset_available',
    poke_interval=60 * 30,
    timeout=60 * 60 * 12,
    ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
    datasets=[plutus_dataset]
)

plutus_campaign_backoff_cluster = EmrClusterTask(
    name="plutus_campaign_backoff_cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[instance_type.with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            instance_type.with_ebs_size_gb(base_ebs_size).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_16xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=on_demand_weighted_capacity,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5_5,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }],
    enable_prometheus_monitoring=True
)

platformwide_stats_job = EmrJobTask(
    name="PlatformWideStatsJob",
    class_name="job.campaignbackoff.PlatformWideStatsJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

campaign_bbf_floor_buffer_selection_job = EmrJobTask(
    name="CampaignBbfFloorBufferCandidateSelectionJob",
    class_name="job.campaignbackoff.CampaignBbfFloorBufferCandidateSelectionJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}"), ("fileCount", f"{fileCount}"), ("testSplit", "0.9"),
                                       ("underdeliveryFraction", "0.02"), ("throttle", "0.8"), ("rollbackUnderdeliveryFraction", "0.05"),
                                       ("rollbackThrottle", "0.8"), ("openMarketShare", "0.01"), ("openPathShare", "0.01")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

campaign_adjustments_job = EmrJobTask(
    name="CampaignAdjustmentsJob",
    class_name="job.campaignbackoff.CampaignAdjustmentsJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}"), ("testSplit", "0.9"), ("updateAdjustmentsVersion", "complex"),
                                       ("fileCount", f"{fileCount}"), ("underdeliveryThreshold", "0.05")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

plutus_campaign_backoff_cluster.add_parallel_body_task(platformwide_stats_job)
plutus_campaign_backoff_cluster.add_parallel_body_task(campaign_bbf_floor_buffer_selection_job)
plutus_campaign_backoff_cluster.add_parallel_body_task(campaign_adjustments_job)

platformwide_stats_job >> campaign_bbf_floor_buffer_selection_job >> campaign_adjustments_job

plutus_campaign_backoff_etl >> plutus_campaign_backoff_cluster
[daily_dataset_sensor, plutus_dataset_sensor] >> plutus_campaign_backoff_cluster.first_airflow_op()

# Activate logworkflow gate when data is available for s3toSql datamover job
# Only do this for prod Jobs.
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    task_batch_grain_daily = 100002  # LogWorkflow.dbo.fn_Enum_TaskBatchGrain_Daily()
    gating_type_id = 2000365  # LogWorkflow.dbo.fn_enum_GatingType_ImportPredictiveClearingCampaignAdjustments

    import_pc_adjustments_gate_open = OpTask(
        op=PythonOperator(
            task_id='open_lwdb_gate',
            python_callable=create_fn_lwdb_gate_open(task_batch_grain_daily, gating_type_id),
            provide_context=True,
            dag=plutus_campaign_backoff_etl.airflow_dag,
        )
    )

    plutus_campaign_backoff_cluster >> import_pc_adjustments_gate_open
