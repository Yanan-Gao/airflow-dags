import json
import logging
import textwrap
from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from dags.forecast.validation.constants import FORECAST_VALIDATION_JOBS_PATH, FORECAST_VALIDATION_JOBS_WHL_NAME
from dags.forecast.validation.validation_helpers.aws_helper import save_json_to_s3, forecast_aws_connection_id
from dags.forecast.validation.validation_helpers.run_db_queries import run_mssql_query, run_vertica_query
from dags.forecast.validation.validation_helpers.truth_metrics_helper import get_daily_aggregated_truth_metrics, \
    get_cumulative_spend_truth
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6id import R6id
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_pyspark import S3PysparkEmrTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.kubernetes.pod_resources import PodResources
from ttd.slack.slack_groups import FORECAST
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.workers.worker import Workers

job_start_date = datetime(2025, 2, 26)
base_job_name = 'generate-stable-lag-set'
input_date = '{{ dag_run.conf.get("input_date", data_interval_end.strftime("%Y-%m-%d")) }}'
input_date_hour = '{{ dag_run.conf.get("input_date", data_interval_end.strftime("%Y-%m-%d %H:%M")) }}'
# By default, output_date is set to input_date
output_date = '{{ dag_run.conf.get("output_date", dag_run.conf.get("input_date", data_interval_end.strftime("%Y-%m-%d"))) }}'
ttd_env = TtdEnvFactory.get_from_system()

forecasting_databahn_conn_id = "forecasting-databahn-db-conn"
forecasting_ttdglobal_conn_id = "forecasting-ttdglobal-db-conn"
vertica_forecasting_validation_conn_id = "vertica_forecasting_validation"

version = "latest"
whl_path = f"{FORECAST_VALIDATION_JOBS_PATH}/release/{version}/pyspark/{FORECAST_VALIDATION_JOBS_WHL_NAME}"
generate_raw_stable_lag_file_path = f"{FORECAST_VALIDATION_JOBS_PATH}/release/{version}/pyspark/generate_stable_lag_set_jobs/stable_lag_generation_job.py"
sample_stable_lag_file_path = f"{FORECAST_VALIDATION_JOBS_PATH}/release/{version}/pyspark/generate_stable_lag_set_jobs/sample_stable_lag_job.py"

# paths for testing:
# branch_name = "lgr-FORECAST-6391-using-datetime-logical-date"
# whl_path = f"{FORECAST_VALIDATION_JOBS_PATH}/dev/{branch_name}/pyspark/{FORECAST_VALIDATION_JOBS_WHL_NAME}"
# generate_raw_stable_lag_file_path = f"{FORECAST_VALIDATION_JOBS_PATH}/dev/{branch_name}/pyspark/generate_stable_lag_set_jobs/stable_lag_generation_job.py"
# sample_stable_lag_file_path = f"{FORECAST_VALIDATION_JOBS_PATH}/dev/{branch_name}/pyspark/generate_stable_lag_set_jobs/sample_stable_lag_job.py"

emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_5
python_version = "3.11.10"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6id.r6id_2xlarge().with_fleet_weighted_capacity(1),
        R6id.r6id_4xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1
)
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6id.r6id_2xlarge().with_fleet_weighted_capacity(1),
        R6id.r6id_4xlarge().with_fleet_weighted_capacity(2),
        R6id.r6id_8xlarge().with_fleet_weighted_capacity(4),
    ],
    on_demand_weighted_capacity=40  # Ran on DataBricks with auto-scaling on, and it created 19 instances of the 4x machine.
)

# DAG Creation
job_name = f"{base_job_name}"

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval="0 12 * * *",  # Once a day at noon
    tags=[FORECAST.team.jira_team],
    slack_tags=FORECAST.team.jira_team,
    slack_channel="#dev-forecasting-validation-alerts",
    max_active_runs=1,
)

databahn_tables_used = """(
    'campaign_nosql'
    -- , 'advertiser_nosql' Not really needed right now. Will add it back in case we need it
    , 'partner_nosql'
    , 'adgroup_nosql1'
    , 'audience_nosql1'
    , 'ActivityRollup'
    , 'DiffXactData'
    , 'adgroupbidlist_nosql1'
    , 'bidlist_nosql1'
  )"""

are_databahn_sets_updated_query = f"""
    SELECT IIF(COUNT(*) > 0, 0, 1)
    FROM kafkatrack.vw_LatestFullSnapshot
    WHERE parentSourceObject IN {databahn_tables_used}
    AND CONVERT(date, SUBSTRING(S3FolderPath, CHARINDEX('date=', S3FolderPath)+5, 8), 112) < '{input_date}'
"""

are_databahn_sets_updated = OpTask(
    op=SqlSensor(
        task_id="are_databahn_sets_updated",
        conn_id=forecasting_databahn_conn_id,
        sql=are_databahn_sets_updated_query,
        poke_interval=60 * 15,  # 15 minutes
        timeout=60 * 60 * 4,  # 4 hours
        mode="reschedule"
    )
)


def store_databahn_datasets_paths(run_date: str):
    try:
        get_databahn_table_paths_query = f"""
                    select
                    IIF(CHARINDEX('_', parentSourceObject) > 0, LEFT(parentSourceObject, CHARINDEX('_', parentSourceObject) - 1),
                           parentSourceObject) AS table_name,
                    min(S3FolderPath) as S3FolderPath
                    from kafkatrack.vw_FullSnapshotFilesAll
                    where
                        LogTime between CAST('{run_date}' AS DATE)
                        and DATEADD(day, 1, CAST('{run_date}' AS DATE))
                        and parentSourceObject in {databahn_tables_used}
                    group by ParentSourceObject
                """

        logging.info(f"Running query to get databahn latest paths:\n {get_databahn_table_paths_query}")
        rows, columns = run_mssql_query(get_databahn_table_paths_query, forecasting_databahn_conn_id)

        logging.info("Creating JSON from databahn query result.")
        data = [dict(zip(columns, row)) for row in rows]
        json_data = json.dumps(data, indent=4)

        logging.info("Saving JSON to S3.")
        save_json_to_s3(
            json_data=json_data,
            key=f'env={ttd_env.dataset_write_env}/forecast-validation/databahn-set-paths/date={run_date}/databahn-paths.json'
        )
    except Exception as e:
        logging.error(f"Failed storing databahn dataset paths with exception: {e}")
        raise


store_databahn_datasets_paths_task = OpTask(
    op=PythonOperator(
        task_id='store_databahn_paths',
        op_kwargs=dict(run_date='{{ data_interval_end.strftime("%Y-%m-%d") }}'),
        python_callable=store_databahn_datasets_paths,
        provide_context=True,
    )
)

is_rti_table_recent_query = f"""
    declare @temp as table (TrueUpWatermarkTime datetime);

    insert into @temp
        exec staging.prc_GetTrueUpWatermark
            @verticaCluster = 9,
            @phaseOriginId = 12,
            @exportBatchTypeId = 16,
            @phaseOriginType = 1;

    SELECT
        CASE
            WHEN TrueUpWatermarkTime >= DATEADD(HOUR, -6, '{input_date_hour}') THEN CAST(1 AS BIT)
            ELSE CAST(0 AS BIT)
        END AS IsRecent
    FROM @temp;
"""

is_rti_table_recent = OpTask(
    op=SqlSensor(
        task_id="is_rti_table_recent",
        conn_id=forecasting_ttdglobal_conn_id,
        sql=is_rti_table_recent_query,
        poke_interval=60 * 15,  # 15 minutes
        timeout=60 * 60 * 4,  # 4 hours
        mode="reschedule"
    )
)

rti_true_up_task_id = 'push_rti_true_up_date'
true_up_xcom_key = 'forecasting_rti_true_up_date'


def push_rti_true_up_date(**context):
    try:
        get_rti_true_up_date = """
                    exec staging.prc_GetTrueUpWatermark
                        @verticaCluster = 9,
                        @phaseOriginId = 12,
                        @exportBatchTypeId = 16,
                        @phaseOriginType = 1;
                """

        logging.info(f"Running query to get rti true up date:\n {get_rti_true_up_date}")
        rows, columns = run_mssql_query(get_rti_true_up_date, forecasting_ttdglobal_conn_id)

        logging.info("Getting true up date...")
        data = [dict(zip(columns, row)) for row in rows]
        true_up_dt = data[0].get("TrueUpWatermarkTime") if data else None
        true_up_str = true_up_dt.strftime("%Y-%m-%d %H:%M") if true_up_dt else None
        logging.info(f"True up date: {true_up_str}")
        context['ti'].xcom_push(key=true_up_xcom_key, value=true_up_str)
        logging.info(
            f"True up date pushed is {context['ti'].xcom_pull(dag_id=job_name, task_ids=rti_true_up_task_id, key=true_up_xcom_key)}"
        )

    except Exception as e:
        logging.error(f"Failed pushing rti true up date: {e}")
        raise


push_rti_true_up_date_task = OpTask(
    op=PythonOperator(
        task_id=rti_true_up_task_id,
        python_callable=push_rti_true_up_date,
        provide_context=True,
    )
)

cluster_task = EmrClusterTask(
    name=job_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": FORECAST.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_auto_terminates=False,
    emr_release_label=emr_version,
    whls_to_install=[whl_path],
    python_version=python_version,
)


def xcom_template_to_get_value(taskid: str, key: str) -> str:
    global job_name
    return f'{{{{ ' \
           f'task_instance.xcom_pull(dag_id="{job_name}", ' \
           f'task_ids="{taskid}", ' \
           f'key="{key}") ' \
           f'}}}}'


arguments = [
    f"--ttd_env={ttd_env.execution_env}", f"--date={xcom_template_to_get_value(rti_true_up_task_id, true_up_xcom_key)}",
    f"--output_date={output_date}"
]

generate_raw_stable_lag_task = S3PysparkEmrTask(
    name="generate-raw-stable-lag-emr",
    entry_point_path=generate_raw_stable_lag_file_path,
    additional_args_option_pairs_list=[("conf", "spark.sql.execution.arrow.pyspark.enabled=true"), ("conf", "spark.driver.memory=16G"),
                                       ("conf", "spark.driver.cores=8")],
    cluster_specs=cluster_task.cluster_specs,
    command_line_arguments=arguments,
)

cluster_task.add_parallel_body_task(generate_raw_stable_lag_task)


# Query reasoning is documented in the Tickets:
# * https://thetradedesk.atlassian.net/browse/FORECAST-6056
# * https://thetradedesk.atlassian.net/browse/FORECAST-6111
# * https://thetradedesk.atlassian.net/browse/FORECAST-6181
def generate_truth_metrics(run_date: str):
    try:
        import pandas as pd

        logging.info(f"Getting minimum created at for run date {run_date}")

        conn = BaseHook.get_connection(forecast_aws_connection_id)
        access_key = conn.login
        secret_key = conn.password

        aws_auth = f"{access_key}:{secret_key}"

        get_truth_metrics = textwrap.dedent(
            f"""
            ALTER SESSION SET AWSRegion='us-east-1';
            ALTER SESSION SET AWSAuth='{aws_auth}';

            drop table if exists tmp_stable_adgroup_metadata;

            CREATE LOCAL TEMPORARY TABLE tmp_stable_adgroup_metadata (
                AdGroupId VARCHAR(32) NOT NULL,
                stableness_start_date_rounded_down TIMESTAMP,
                stableness_length_rounded_down INT,
                PRIMARY KEY (AdGroupId)
            ) ON COMMIT PRESERVE ROWS
            SEGMENTED BY HASH(AdGroupId) ALL NODES;

            COPY tmp_stable_adgroup_metadata FROM 's3://ttd-forecasting-useast/env={ttd_env.dataset_write_env}/forecast-validation/processed-stable-adgroups-metadata/stable-adgroups-{run_date}/*.csv'
            ABORT ON ERROR DELIMITER ',' SKIP 1;

            WITH seq AS (
                /* Generate sequence of daily intervals using TIMESERIES */
                SELECT ROW_NUMBER() OVER() - 1 AS i
                FROM (
                    SELECT 1 FROM (
                        SELECT DATE(0) + INTERVAL '1 second' AS se
                        UNION ALL
                        SELECT DATE(0) + INTERVAL '1000 seconds' AS se
                    ) a
                    TIMESERIES tm AS '1 second' OVER (ORDER BY se)
                ) b
            ),
            ExceedingSpenders AS (
                /* Flag ad groups with spending before stable period starts */
                SELECT DISTINCT
                    metadata.AdGroupId
                FROM tmp_stable_adgroup_metadata AS metadata
                JOIN reports.ForecastValidationTruthMetrics AS fvtm
                  ON fvtm.AdGroupId = metadata.AdGroupId
                 AND fvtm.ReportHourUtc < metadata.stableness_start_date_rounded_down
                 AND fvtm.AdvertiserCostInUSD > 0
            ),
            StablePeriodDays AS (
                /* Generate daily intervals for stable ad groups */
                SELECT
                    metadata.AdGroupId,
                    metadata.stableness_start_date_rounded_down + (seq.i * INTERVAL '1 day') AS start_date,
                    metadata.stableness_start_date_rounded_down + ((seq.i + 1) * INTERVAL '1 day') AS end_date,
                    seq.i + 1 AS day_number
                FROM tmp_stable_adgroup_metadata AS metadata
                CROSS JOIN seq
                WHERE seq.i < metadata.stableness_length_rounded_down
                  AND metadata.AdGroupId NOT IN (
                      SELECT AdGroupId
                      FROM ExceedingSpenders
                  )
            ),
            AggregatedDailySpend AS (
                /* Aggregate spend and reach metrics for stable periods */
                SELECT
                    sp.AdGroupId,
                    sp.day_number,
                    sp.start_date,
                    sp.end_date,
                    COALESCE(SUM(fvtm.AdvertiserCostInUSD), 0) AS AdvertiserCostInUSD,
                    COALESCE(SUM(fvtm.BidCount), 0) AS BidCount,
                    COALESCE(SUM(fvtm.ImpressionCount), 0) AS ImpressionCount,
                    COALESCE(SUM(fvtm.BidAmountInUSD), 0) AS BidAmountInUSD,
                    approximate_count_distinct_of_synopsis(fvtm.ImpressionUniquesCountSynopsis2, 5) AS IdReach,
                    approximate_count_distinct_of_synopsis(fvtm.PersonIDUniqueCountsSynopsis2, 5) AS PersonReach,
                    approximate_count_distinct_of_synopsis(fvtm.CTVHHIDUniqueCountsSynopsis2, 5) AS HouseholdReach
                FROM StablePeriodDays AS sp
                LEFT JOIN reports.ForecastValidationTruthMetrics AS fvtm
                    ON fvtm.AdGroupId = sp.AdGroupId
                       AND fvtm.ReportHourUtc >= sp.start_date AND fvtm.ReportHourUtc < sp.end_date
                GROUP BY sp.AdGroupId, sp.day_number, sp.start_date, sp.end_date
            )
            SELECT
                AdGroupId,
                day_number,
                start_date,
                end_date,
                AdvertiserCostInUSD,
                BidCount,
                ImpressionCount,
                BidAmountInUSD,
                IdReach,
                PersonReach,
                HouseholdReach
            FROM AggregatedDailySpend
            ORDER BY AdGroupId, day_number;
        """
        )

        logging.info("Running vertica query to get truth metrics for raw stable AdGroupIds.")
        rows, columns = run_vertica_query(query=get_truth_metrics)

        logging.info("Creating pandas dataframe from vertica query output.")
        df = pd.DataFrame(rows, columns=columns)

        numeric_columns = [
            'AdvertiserCostInUSD', 'BidCount', 'ImpressionCount', 'BidAmountInUSD', 'IdReach', 'PersonReach', 'HouseholdReach'
        ]
        logging.info(f"Casting numeric columns ({numeric_columns}) to numeric.")
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

        logging.info("Sorting df by AdGroupId and DayNumber.")
        df.sort_values(['AdGroupId', 'day_number'], inplace=True)

        logging.info("Getting daily aggregated truth metrics.")
        aggregated_truth_metrics = get_daily_aggregated_truth_metrics(df)
        logging.info("Getting aggregated cumulative spend truth metrics.")
        cumulative_spend_truth = get_cumulative_spend_truth(df)

        logging.info("Creating JSON of daily aggregated truth metrics.")
        json_data_aggregated_daily = json.dumps(aggregated_truth_metrics.to_dict(orient='records'), indent=4, default=str)

        logging.info("Saving JSON of daily aggregated truth metrics to S3.")
        save_json_to_s3(
            json_data=json_data_aggregated_daily,
            key=
            f'env={ttd_env.dataset_write_env}/forecast-validation/raw-stable-adgroups-truth-metrics/date={run_date}/daily-full-truth-metrics.json'
        )

        logging.info("Creating JSON of aggregated cumulative spend truth metrics.")
        json_data_cumulative_spend = json.dumps(cumulative_spend_truth.to_dict(orient='records'), indent=4, default=str)

        logging.info("Saving aggregated cumulative spend truth metrics to S3.")
        save_json_to_s3(
            json_data=json_data_cumulative_spend,
            key=
            f'env={ttd_env.dataset_write_env}/forecast-validation/raw-stable-adgroups-spend-metrics/date={run_date}/cumulative-spend-metrics.json'
        )

    except Exception as e:
        logging.error(f"Failed generating raw stable adgroups truth metrics with exception: {e}")
        raise


generate_truth_metrics_step = OpTask(
    op=PythonOperator(
        task_id='generate_truth_metrics',
        retries=3,
        op_kwargs=dict(run_date='{{ data_interval_end.strftime("%Y-%m-%d") }}'),
        python_callable=generate_truth_metrics,
        queue=Workers.k8s.queue,
        pool=Workers.k8s.pool,
        executor_config=PodResources(request_cpu="1", request_memory="2Gi", limit_memory="4Gi").as_executor_config(),
    )
)

sample_rate = 20  # one in how many records do we save (ie we keep 1 in 20 adgroups)
sample_stable_lag_task = S3PysparkEmrTask(
    name="sample-stable-lag-emr",
    entry_point_path=sample_stable_lag_file_path,
    additional_args_option_pairs_list=[("conf", "spark.sql.execution.arrow.pyspark.enabled=true"), ("conf", "spark.driver.memory=16G"),
                                       ("conf", "spark.driver.cores=8")],
    cluster_specs=cluster_task.cluster_specs,
    command_line_arguments=arguments + [f"--sample_rate={sample_rate}"],
)

cluster_task.add_parallel_body_task(sample_stable_lag_task)

ttd_dag >> are_databahn_sets_updated >> store_databahn_datasets_paths_task
are_databahn_sets_updated >> is_rti_table_recent >> push_rti_true_up_date_task
push_rti_true_up_date_task >> cluster_task
store_databahn_datasets_paths_task >> cluster_task
generate_raw_stable_lag_task >> generate_truth_metrics_step >> sample_stable_lag_task

stable_lag_generate_dag = ttd_dag.airflow_dag
