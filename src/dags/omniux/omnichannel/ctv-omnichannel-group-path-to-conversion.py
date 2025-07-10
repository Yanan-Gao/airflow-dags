import logging
from dags.omniux.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datasources.datasources import Datasources
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import OMNIUX
from datetime import datetime, timedelta
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.interop.logworkflow_callables import ExecuteOnDemandDataMove

# Scheduled run - default
default_attribution_window = 14
default_impression_window = 28
last_check_lookback_days = 2  # Lookback day count to from run date, and is the last date checked by the data sensor, where bf and ae must be present
default_end_date_impressions_lookback_days = last_check_lookback_days - 1  # minus 1 as end date is exclusive
default_start_date_lookback_days = default_impression_window + default_end_date_impressions_lookback_days

spark_options = [
    ("conf", "spark.driver.maxResultSize=0"),
    ("conf", "spark.network.timeout=720s"),  # default: 120s
    ("conf", "spark.executor.heartbeatInterval=30s"),  # default: 10s
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    (
        "conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
    ("conf", "spark.eventLog.rolling.enabled=false"),
    ("conf", "spark.sql.files.maxPartitionBytes=1073741824"),  # 1GB
    ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
]

dag_id = "ctv-omnichannel-group-path-to-conversion"
get_emr_task_args_task_id = "get-emr-task-arguments"

run_datetime_format = "%Y-%m-%dT%H:00:00"
ds_date_format = "%Y-%m-%d %H:00:00"
run_date_format = "%Y-%m-%d"

env = TtdEnvFactory.get_from_system()

# LWDB configs
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"

env_dir = "prod"
if env == TtdEnvFactory.prod:
    slack_tags = OMNIUX.omniux().sub_team
    enable_slack_alert = True
    logworkflow_connection_open_gate = logworkflow_connection
else:
    slack_tags = None
    enable_slack_alert = False
    env_dir = "test"
    logworkflow_connection_open_gate = logworkflow_sandbox_connection  # sb connection does not seem to work, use logworkflow_connection cautiously


def xcom_pull_expr(key: str) -> str:
    return (f"{{{{ "
            f'task_instance.xcom_pull(dag_id="{dag_id}", '
            f'task_ids="{get_emr_task_args_task_id}", '
            f'key="{key}") '
            f"}}}}")


# Manually trigger job with args as:
# {"start_date": "2024-06-29", "end_date_impressions": "2024-07-28", "attribution_window": "60", "run_datetime": "2024-10-02T12:01:02"}
start_date_key = "start_date"
end_date_impressions_key = "end_date_impressions"
attribution_window_key = "attribution_window"
run_date_time_key = "run_datetime"
max_check_date_key = "max_check_date"

ttd_ctv_bucket = "ttd-ctv"
java_options = [
    # ("advertiserIds", "hcx64wq,zo3cxwj"),
    ("rootGroupPathToConversionPath", f"s3a://{ttd_ctv_bucket}"),
    ("runDateTime", xcom_pull_expr(run_date_time_key)),
    ("attributionWindow", xcom_pull_expr(attribution_window_key)),
    ("startDate", xcom_pull_expr(start_date_key)),
    ("endDateImpressions", xcom_pull_expr(end_date_impressions_key)),
    ("pipeline", "Group"),
    ("ttd.ds.PtcDataset.isInChain", "true"),
]

job_start_date = datetime(2025, 4, 13, 0, 0)
job_schedule_interval = "@weekly"
retries = 0
job_retry_delay = timedelta(hours=4)
job_map = {"report-input": "ReportInput", "impression": "ImpressionPath", "conversion": "ConversionPath", "merge": "MergePath"}
# region add steps to dag
eldorado_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=retries,
    retry_delay=job_retry_delay,
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    tags=[OMNIUX.team.name, "omnichannel", "omnichannel-outcomes"],
    run_only_latest=True,
)
dag = eldorado_dag.airflow_dag


def get_emr_task_args(**context):
    dag_run = context["dag_run"]

    if dag_run.run_type == "manual":
        logging.info('Manually triggered.')
        run_date_time_str = dag_run.conf.get(run_date_time_key, datetime.utcnow().strftime(run_datetime_format))
        run_date_time = datetime.strptime(run_date_time_str, run_datetime_format)
    else:
        logging.info('Scheduled run.')
        run_date_time = context["data_interval_end"]
        run_date_time_str = run_date_time.strftime(run_datetime_format)

    max_check_date_str = (run_date_time - timedelta(days=last_check_lookback_days)).strftime(ds_date_format)
    default_start_date = (run_date_time - timedelta(days=default_start_date_lookback_days)).strftime(run_date_format)
    default_end_date_impressions = (run_date_time - timedelta(days=default_end_date_impressions_lookback_days)).strftime(run_date_format)

    defaults = {
        start_date_key: default_start_date,
        end_date_impressions_key: default_end_date_impressions,
        attribution_window_key: f"{default_attribution_window}"
    }
    xcom_data = {key: dag_run.conf.get(key, defaults[key]) for key in defaults}
    xcom_data[run_date_time_key] = run_date_time_str
    xcom_data[max_check_date_key] = max_check_date_str

    task_instance = context["task_instance"]
    for key, value in xcom_data.items():
        task_instance.xcom_push(key=key, value=value)
        logging.info(f"Pushed '{key}': {value}")


def emr_task(name, class_name, on_demand_weighted_capacity, master_instance_type=M5d.m5d_8xlarge()):
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            # 24xlarge Instances
            R5a.r5a_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_24xlarge().cores),
            R5.r5_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_24xlarge().cores),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_24xlarge().cores),
            # 16xlarge Instances
            R5a.r5a_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_16xlarge().cores),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_16xlarge().cores),
            R5.r5_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_16xlarge().cores),
            # 12xlarge Instances
            R5a.r5a_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_12xlarge().cores),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_12xlarge().cores),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5.r5_12xlarge().cores),
            # 8xlarge Instances
            R5a.r5a_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5a.r5a_8xlarge().cores),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R5d.r5d_8xlarge().cores),
        ],
        on_demand_weighted_capacity=int(on_demand_weighted_capacity * 96),
    )

    # Define Cluster
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[master_instance_type.with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    cluster = EmrClusterTask(
        name=f"process-{name}-path",
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags={"Team": OMNIUX.team.jira_team},
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        retries=0,
    )

    # Add step to cluster
    step = EmrJobTask(
        name=f"process-{name}-path_step",
        class_name=class_name,
        executable_path=get_jar_file_path(),
        configure_cluster_automatically=True,
        additional_args_option_pairs_list=spark_options,
        eldorado_config_option_pairs_list=java_options + [("jobs", job_map.get(name, name))],
    )
    cluster.add_parallel_body_task(step)
    return cluster


get_emr_task_args_step = OpTask(
    op=PythonOperator(
        task_id=get_emr_task_args_task_id,
        python_callable=get_emr_task_args,
        dag=dag,
        provide_context=True,
    )
)

check_latest_bfs_aes_exist = OpTask(
    op=DatasetCheckSensor(
        task_id="check_latest_bfs_aes_exist",
        datasets=[
            RtbDatalakeDatasource.rtb_attributedevent_verticaload_v1.with_check_type('day'),
            RtbDatalakeDatasource.rtb_attributedeventresult_verticaload_v1.with_check_type('day'),
            Datasources.ctv.bidfeedback_daily_subset_no_partition(env.dataset_read_env, version=1)
        ],
        ds_date=xcom_pull_expr(max_check_date_key),
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 20,  # wait up to 12 hours,
        dag=dag
    )
)


def ExecuteOnDemandDataMoveWrapper(**context):
    run_date_time_str = context['task_instance'].xcom_pull(dag_id=dag_id, task_ids=get_emr_task_args_task_id, key=run_date_time_key)
    logging.info(f"Try to reformat runDatetime to a format that matches S3 path: {run_date_time_str}")

    run_date_time = datetime.strptime(run_date_time_str, "%Y-%m-%dT%H:%M:%S")
    formatted_str = f'date={run_date_time.strftime("%Y%m%d")}/hour={run_date_time.strftime("%H")}/'  # 'date={YYYYMMDD}/hour={HH}'
    logging.info(f"Datamover import datetime directory: {formatted_str} ...")

    op_kwargs = {
        'mssql_conn_id': logworkflow_connection_open_gate,
        'sproc_arguments': {
            'taskId': 1000733,  # dbo.fn_Enum_Task_OnDemandImportOmnichannelGroupPathToConversion()
            'prefix': formatted_str
        }
    }
    logging.info("Call ExecuteOnDemandDataMove ...")
    ExecuteOnDemandDataMove(**op_kwargs)


logworkflow_open_sql_import_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExecuteOnDemandDataMoveWrapper,
        task_id="logworkflow_group_ptc_sql_import_on_demand",
        trigger_rule=TriggerRule.ONE_SUCCESS,
        provide_context=True,
    )
)

report_input_task_cluster = emr_task(
    'report-input', 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 1, master_instance_type=M5d.m5d_2xlarge()
)
impression_path_cluster = emr_task("impression", 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 30)
conversion_path_cluster = emr_task("conversion", 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 30)
merge_task_cluster = emr_task(
    "merge", 'com.thetradedesk.ctv.upstreaminsights.pipelines.ptc.PtcJob', 1, master_instance_type=M5d.m5d_2xlarge()
)

eldorado_dag >> get_emr_task_args_step >> check_latest_bfs_aes_exist >> report_input_task_cluster
report_input_task_cluster >> impression_path_cluster >> merge_task_cluster
report_input_task_cluster >> conversion_path_cluster >> merge_task_cluster
merge_task_cluster >> logworkflow_open_sql_import_gate
