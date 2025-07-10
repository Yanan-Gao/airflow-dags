"""
Defines the Airflow dag for the daily IO-Consolidation Avails Processing job.
"""

from datetime import datetime, timedelta

from dags.cmo.utils.fleet_batch_config import getMasterFleetInstances, EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

jar_path = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/ctv/upstreaminsights_2.12-assembly.jar"
job_start_date = datetime(2025, 5, 28, 0, 0)

schedule_interval = '@daily'
pipeline_name = "ctv-io-consolidation-avails-processing_v3"

avails_job_name = "avails-processing"
avails_processing_class_name = "com.thetradedesk.ctv.upstreaminsights.pipelines.ioconsolidation" \
                                ".AvailsProcessingV2"
rollup_class_name = "com.thetradedesk.ctv.upstreaminsights.pipelines.ioconsolidation.AvailRollingAggregator"
start_date = '{{ dag_run.conf.get("startDate", data_interval_start.strftime("%Y-%m-%d")) }}'
end_date = '{{ dag_run.conf.get("endDate", data_interval_end.strftime("%Y-%m-%d")) }}'
ds_date = '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}'
lookback = 0  # due to [start_date, end_Date)

spark_options = [
    ("conf", "spark.driver.maxResultSize=0"),
    ("conf", "spark.network.timeout=6000s"),
    ("conf", "spark.executor.heartbeatInterval=5000s"),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
]
java_options = [
    ("runDate", start_date),
    ("s3RootPath", "s3://ttd-publisher-insights"),
    # ("ttd.ds.IOConsolidationAvailsDatasetV2.isInChain", "true")
]
# region add steps to dag
eldorado_dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=schedule_interval,
    retries=1,
    retry_delay=timedelta(hours=4),
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "upstreaminsights", "reach and overlap"],
)
dag = eldorado_dag.airflow_dag

# Ref: https://thetradedesk.gitlab-pages.adsrvr.org/teams/aifun/TTD.AvailsPipeline.Contract/consuming/prod/#airflow-orchestration
wait_for_input_data = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id="wait-for-identity-agg-daily",
        ds_date=ds_date,
        lookback=0,
        poke_interval=60 * 10,
        timeout=60 * 60 * 24,
        datasets=[AvailsDatasources.identity_agg_daily_dataset]
    )
)

master_fleet_instance_type_configs = getMasterFleetInstances(EmrInstanceClasses.NetworkOptimized, EmrInstanceSizes.TwoX)

core_fleet_instance_type_configs = getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.SixteenX, instance_capacity=30)

# Define Cluster task
avails_aggregation_cluster = EmrClusterTask(
    name=avails_job_name,
    cluster_tags={"Team": PFX.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
)

# Add job to Cluster task
job_task = EmrJobTask(
    name=avails_job_name + "_step",
    class_name=avails_processing_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=java_options,
    timeout_timedelta=timedelta(minutes=600),
)

# endregion add steps to dag

# region define workflow

avails_aggregation_cluster.add_parallel_body_task(job_task)
eldorado_dag >> wait_for_input_data >> avails_aggregation_cluster

# endregion define workflow
