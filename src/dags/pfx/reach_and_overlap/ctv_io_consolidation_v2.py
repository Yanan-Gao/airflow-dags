import re
from datetime import datetime, timedelta

from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.python import ShortCircuitOperator

from datasources.datasources import Datasources
from datasources.sources.ctv_datasources import CtvDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.storage_optimized.i3en import I3en
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import PFX
from ttd.ttdenv import TtdEnvFactory

# region job configurations

jar_path = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/ctv/upstreaminsights_2.12-assembly.jar"
job_start_date = datetime(2024, 4, 28, 0, 0)

run_id_cleaned = '{{ task_instance.xcom_pull(task_ids="check_queue",key="run_id_cleaned")}}'
job_environment = TtdEnvFactory.get_from_system()
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5

sib_lookback_weeks = 4
athena_output_table = f"io_consolidation_v5_{job_environment.dataset_write_env}"
athena_output_location = f"s3://ttd-publisher-insights/env={job_environment.dataset_write_env}/IOConsolidation/athena-output"

# audience queue paths for on-demand job
bucket_without_s3 = "ttd-publisher-insights"

# See: https://stackoverflow.com/questions/42876195/avoid-creation-of-folder-keys-in-s3-with-hadoop-emr
# Use s3a to avoid creation of files like "*$folder$" on S3
bucket = f"s3a://{bucket_without_s3}"

audience_queue_prefix = f"env={job_environment.dataset_write_env}/IOConsolidation/v=5/audiencequeue"
queued_prefix = f"{audience_queue_prefix}/status=queued"
start_date = '{{ dag_run.conf.get("startDate", macros.ds_add(next_ds, -33)) }}'
end_date = '{{ dag_run.conf.get("endDate", macros.ds_add(next_ds, -3)) }}'

audience_job_name = "audience-processing"
io_consolidation_job_name = "io-consolidation-aggregation"
audience_processing_class_name = "com.thetradedesk.ctv.upstreaminsights.pipelines.ioconsolidation.AudienceProcessing"
io_consolidation_class_name = "com.thetradedesk.ctv.upstreaminsights.pipelines.ioconsolidation" \
                              ".IOConsolidationAggregation"

shared_spark_options = [("conf", "spark.driver.maxResultSize=0"), ("conf", "spark.network.timeout=1440s"),
                        ("conf", "spark.executor.heartbeatInterval=120s")]

shared_java_options = [("ioConsolidationPath", CtvDatasources.io_consolidation_v2.as_write().get_root_path()), ("startDate", start_date),
                       ("endDate", end_date), ("sibLookbackWeeks", sib_lookback_weeks), ("ThirdPartyDataProviderId", "thetradedesk"),
                       ("filterSellerByLastBidDate", "true"), ("lastBidLookbackDays", 30),
                       ("ttd.ds.AudienceUserDataset.isInChain", "false"), ("ttd.ds.IOConsolidationAvailsDataset.isInChain", "false")]

daily_java_options = shared_java_options + [("mode", "on-demand"), ("audienceFileRootPath", f"{bucket}/{audience_queue_prefix}"),
                                            ("runId", run_id_cleaned)]
# endregion job configurations


def check_queue(**kwargs):
    """
    Checks the audience queue to see if there are any files to be processed.
    """
    run_id = re.sub(r'[^a-zA-Z\d]+', '-', kwargs['run_id'])
    # Push the cleaned run_id, so it can be passed to the on-demand job
    kwargs['task_instance'].xcom_push(key="run_id_cleaned", value=run_id)
    aws_storage = AwsCloudStorage(conn_id='aws_default')

    print(f"Fetching files from bucket: {bucket_without_s3} and prefix: {queued_prefix}")

    # fetch files in queue
    audience_files = aws_storage.list_keys(
        bucket_name=bucket_without_s3,
        prefix=queued_prefix,
    )

    # Weird bug where list_keys return different values if folder was created from console vs S3 Hook.
    # To avoid, filter out any unwanted directories
    # https://stackoverflow.com/questions/9954521/s3-boto-list-keys-sometimes-returns-directory-key
    audience_files = list(filter(lambda path: not path.endswith('/'), audience_files))

    # if no files are found, return None to ShortCircuitOperator to stop the dag
    if len(audience_files) == 0:
        return None
    else:
        return True


def create_cluster_and_step(dag: TtdDag, spark_options, java_options, on_demand=False):
    """
    Creates cluster and steps that are shared between scheduled and on-demand IOConsolidation jobs.
    """
    # This is a very loose recency operator, we cannot guarantee that all 30 days of avails data will exist.
    # The upstream job ctv-io-consolidation-avails-processing has a much stricter avails check.
    airflow_dag = dag.airflow_dag
    check_data = DatasetCheckSensor(
        dag=airflow_dag,
        datasets=[CtvDatasources.io_consolidation_30days_aggregated_avails,
                  Datasources.sib.sibv2_group_seven_day_rollup(11)],  # This is to make sure we use legacy sib data
        # lookback=30,
        ds_date='{{(data_interval_end - macros.timedelta(days=4)).to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12
    )

    update_io_consolidation_athena_table = AWSAthenaOperator(
        task_id='update_io_consolidation_athena_table',
        output_location=athena_output_location,
        query=f'MSCK REPAIR TABLE {athena_output_table};',
        retries=3,
        sleep_time=60,
        aws_conn_id='aws_default',
        database='ctv_db',
        dag=airflow_dag
    )

    # region Audience Processing

    # Define Cluster
    audience_processing_cluster = EmrClusterTask(
        name=audience_job_name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[R7g.r7g_16xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M7g.m7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=22
        ),
        cluster_tags={"Team": PFX.team.jira_team},
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        emr_release_label=emr_release_label,
    )

    # Add step to cluster
    step = EmrJobTask(
        name=audience_job_name + "_step",
        class_name=audience_processing_class_name,
        executable_path=jar_path,
        configure_cluster_automatically=True,
        additional_args_option_pairs_list=spark_options,
        eldorado_config_option_pairs_list=java_options
    )
    audience_processing_cluster.add_parallel_body_task(step)
    # endregion Audience Processing

    # region IOConsolidationAggregation

    # Define Cluster
    io_consolidation_aggregation_cluster = EmrClusterTask(
        name=io_consolidation_job_name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[I3en.i3en_24xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[I3en.i3en_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=30
        ),
        cluster_tags={"Team": PFX.team.jira_team},
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        emr_release_label=emr_release_label
    )

    # Add Step to Cluster
    step = EmrJobTask(
        name=io_consolidation_job_name + "_step",
        class_name=io_consolidation_class_name,
        executable_path=jar_path,
        configure_cluster_automatically=True,
        additional_args_option_pairs_list=spark_options,
        eldorado_config_option_pairs_list=java_options
    )
    io_consolidation_aggregation_cluster.add_parallel_body_task(step)

    # endregion Avails Aggregation
    dag >> audience_processing_cluster >> io_consolidation_aggregation_cluster

    if on_demand:
        check_queue_op = on_demand_operators(dag)
        check_queue_op >> check_data >> audience_processing_cluster.first_airflow_op()
        io_consolidation_aggregation_cluster.last_airflow_op()
    else:
        check_data >> audience_processing_cluster.first_airflow_op()
    io_consolidation_aggregation_cluster.last_airflow_op() >> update_io_consolidation_athena_table
    return dag


def on_demand_operators(dag):
    check_queue_op = ShortCircuitOperator(
        task_id="check_queue", provide_context=True, python_callable=check_queue, dag=dag.airflow_dag, retries=0
    )
    return check_queue_op


def create_io_consolidation_dag(dag_id, interval, on_demand):
    dag = TtdDag(
        dag_id=dag_id,
        start_date=job_start_date,
        schedule_interval=interval,
        retries=1,
        retry_delay=timedelta(hours=4),
        slack_channel=PFX.team.alarm_channel,
        tags=[PFX.team.name, "reach and overlap"],
        run_only_latest=True
    )

    return create_cluster_and_step(dag, shared_spark_options, daily_java_options if on_demand else shared_java_options, on_demand)


monthly_job_schedule_interval = '0 0 1 * *'
monthly_pipeline_name = "ctv-io-consolidation-scheduled_v2"
globals()[monthly_pipeline_name] = create_io_consolidation_dag(monthly_pipeline_name, monthly_job_schedule_interval, False).airflow_dag

daily_job_schedule_interval = '0 0 * * *'
daily_pipeline_name = "ctv-io-consolidation-on-demand_v2"

globals()[daily_pipeline_name] = create_io_consolidation_dag(daily_pipeline_name, daily_job_schedule_interval, True).airflow_dag
