import copy
from datetime import datetime, timedelta

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
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [("executor-memory", "203G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=128G"),
                      ("conf", "spark.driver.cores=20"), ("conf", "spark.sql.shuffle.partitions=4096"),
                      ("conf", "spark.default.parallelism=4096"), ("conf", "spark.driver.maxResultSize=80G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25"), ("conf", "spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED"),
                      ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

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
next_date = "{{ (data_interval_start + macros.timedelta(days=1)).strftime(\"%Y%m%d\") }}"
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/yxz-AUDAUTO-2179-erm-graph-policy/latest/audience.jar"

audience_policy_table_etl_dag = TtdDag(
    dag_id="perf-automation-erm-model-policy-table-etl",
    start_date=datetime(2024, 7, 20, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "ERM"]
)

dag = audience_policy_table_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
bidsimpressions_data = HourGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/koav4/v=1/prod",
    data_name="bidsimpressions",
    date_format="year=%Y/month=%m/day=%d",
    hour_format="hourPart={hour}",
    env_aware=False,
    version=None,
)
# s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/trackingtag/v=1/date=20240418/
tracking_tag_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="trackingtag",
    version=1,
    env_aware=False,
    success_file=None,
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[bidsimpressions_data, tracking_tag_dataset],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

###############################################################################
# clusters
###############################################################################
audience_policy_table_etl_cluster_task = EmrClusterTask(
    name="AudienceERMPolicyTableCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32)],
        on_demand_weighted_capacity=1920
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

###############################################################################
# steps
###############################################################################
pixels_config = f"'s3://thetradedesk-useast-hadoop/Data_Science/Yang/audience_extension/fake_data/erm_blockblast_selected_seed/{next_date}/'"
audience_erm_policy_table_generation_step = EmrJobTask(
    name="AudiencePolicyTableGenerator",
    class_name="com.thetradedesk.audience.jobs.policytable.AudiencePolicyTableGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[('modelName', "AEM"), ('date', run_date), ('userDownSampleHitPopulationAEM', '200000'),
                                       ('conversionLookBack', '7'), ('bidImpressionLookBack', '0'), ('saltToSampleUserAEM', '0BgGCE'),
                                       ('policyTableResetSyntheticId', 'false'), ('seedCoalesceAfterFilter', '32'),
                                       ('bidImpressionRepartitionNum', '4096'), ('useSelectedPixel', 'true'),
                                       ('selectedPixelsConfigPath', pixels_config)],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=4)
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

audience_policy_table_etl_cluster_task.add_parallel_body_task(audience_erm_policy_table_generation_step)

audience_policy_table_etl_dag >> dataset_sensor >> audience_policy_table_etl_cluster_task >> final_dag_status_step
