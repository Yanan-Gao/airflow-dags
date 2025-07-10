from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from dags.forecast.columnstore.columnstore_common_setup import ColumnStoreDAGSetup, ColumnStoreEmrSetup
from dags.forecast.columnstore.columnstore_sql import ColumnStoreFunctions
from dags.forecast.columnstore.enums.table_type import TableType
from dags.forecast.columnstore.enums.xd_level import XdLevel
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

job_name = "uf-columnstore-relevance-data-generator"
job_start_date = datetime(year=2025, month=2, day=16, hour=2)
# Every day at 12 o'clock - allows 2 hours for no-xd data to generate.
job_schedule_interval = "0 12 * * *"
active_running_jobs = 1
columnstore_dag_common_parameters = ColumnStoreDAGSetup()
columnstore_sql_functions = ColumnStoreFunctions()
emr_steps_common_parameters = ColumnStoreEmrSetup()
spark_executable_path = emr_steps_common_parameters.job_jar

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=columnstore_dag_common_parameters.slack_channel,
    tags=columnstore_dag_common_parameters.tags,
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30),
    default_args=columnstore_dag_common_parameters.default_args,
    dag_tsg=columnstore_dag_common_parameters.dag_tsg,
    run_only_latest=True,
)

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_xlarge().with_ebs_size_gb(50).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

# The jobs are fairly small, and as such don't need a huge amount of resources to run
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C5.c5_4xlarge().with_fleet_weighted_capacity(16),
        C5.c5_9xlarge().with_fleet_weighted_capacity(36),
        C5.c5_12xlarge().with_fleet_weighted_capacity(48),
        M6g.m6g_16xlarge()
    ],
    on_demand_weighted_capacity=640,
)

date = """{{ data_interval_start.subtract(days=1).to_date_string() }}"""


def generate_id_filtering_cluster(date: str) -> EmrClusterTask:
    data_filtering_cluster = EmrClusterTask(
        name="uf_columnstore_relevance_data_filtering_cluster",
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
    )

    rsm_id_filtering_task = EmrJobTask(
        name="uf-columnstore-rsm-ids-task",
        class_name="com.thetradedesk.etlforecastjobs.preprocessing.columnstore.relevance.FilterRSMIds",
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("withNewCols", "false"),
            # Has no effect unless in prodTest
            ("ttd.ds.SeenInBiddingDeviceV3IdOnlyDataSet.isInChain", "true"),
            ("ttd.ds.GeronimoIdsByDayDataSet.isInChain", "true"),
            ("ttd.ds.DistinctGeronimoIdsDataSet.isInChain", "true"),
            ("enableLogging", "true")
        ],
        additional_args_option_pairs_list=[("conf", "spark.sql.shuffle.partitions=3000")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=4),
    )

    data_filtering_cluster.add_parallel_body_task(rsm_id_filtering_task)

    return data_filtering_cluster


def generate_rsm_avails_generation_cluster(date: str, support_deals: bool) -> EmrClusterTask:

    with_support_deals_suffix = "_with_deals" if support_deals else ""

    avails_generation_cluster = EmrClusterTask(
        name=f"uf_columnstore_relevance_rsm_avails_generation_cluster{with_support_deals_suffix}",
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
    )

    avails_generation_task = EmrJobTask(
        name="uf-columnstore-relevance-dataset-filtering-task",
        class_name="com.thetradedesk.etlforecastjobs.preprocessing.columnstore.relevance.GenerateFilteredAvailsDatasetsForRsm",
        eldorado_config_option_pairs_list=[
            ("date", date),
            # Has no effect unless in prodTest
            ("ttd.ds.IdsInRsmDataSet.isInChain", "true"),
            ("ttd.ds.RSMIdCombinationAvailsDataset.isInChain", "true"),
            ("ttd.ds.RSMFilteredCombinationIdsDataSet.isInChain", "true"),
            ("withNewCols", "true"),
            ("enableLogging", "true"),
            ("supportDeals", "true" if support_deals else "false")
        ],
        additional_args_option_pairs_list=[("conf", "spark.sql.shuffle.partitions=3000")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=4),
    )

    avails_generation_cluster.add_parallel_body_task(avails_generation_task)

    return avails_generation_cluster


# This dataset will depend on the rollUps without deals that we'll load into Vertica by default.
def generate_rsm_audience_generation_cluster(date: str) -> EmrClusterTask:
    audience_generation_cluster = EmrClusterTask(
        name="uf_columnstore_relevance_rsm_audience_generation_cluster",
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
    )

    audience_generation_task = EmrJobTask(
        name="uf-columnstore-relevance-dataset-filtering-task",
        class_name="com.thetradedesk.etlforecastjobs.preprocessing.columnstore.relevance.GenerateFilteredAudienceDatasetForRsm",
        eldorado_config_option_pairs_list=[
            ("date", date),
            # Has no effect unless in prodTest
            ("ttd.ds.IdsInRsmDataSet.isInChain", "true"),
            ("ttd.ds.RSMIdCombinationAvailsDataset.isInChain", "true"),
            ("ttd.ds.RSMFilteredCombinationIdsDataSet.isInChain", "true"),
            ("withNewCols", "true"),
            ("enableLogging", "true")
        ],
        additional_args_option_pairs_list=[("conf", "spark.sql.shuffle.partitions=3000")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=4),
    )

    audience_generation_cluster.add_parallel_body_task(audience_generation_task)

    return audience_generation_cluster


def generate_external_sensor_task(external_task_id: str, external_dag_id: str, task_id: str, execution_delta: timedelta) -> OpTask:
    return OpTask(
        op=ExternalTaskSensor(
            external_task_id=external_task_id,
            external_dag_id=external_dag_id,
            task_id=task_id,
            allowed_states=["success"],
            check_existence=False,
            poke_interval=30,
            poll_interval=60,
            # Looks at the run for the same day two hours before i.e. at 10 AM.
            execution_delta=execution_delta,
            mode="reschedule",
            dag=dag.airflow_dag
        )
    )


wait_for_audience_dataset_completion = generate_external_sensor_task(
    external_task_id=
    "uf_columnstore_v2_no_xd_level_rollups_cluster-with-relevance-cols_watch_task_uf-columnstore-v2_no_xd_level-id-combinations-audience",
    external_dag_id="uf-columnstore-v2_no_xd_level-data-generator",
    task_id="wait_for_audience_dataset_completion",
    execution_delta=timedelta(hours=2)
)

wait_for_rollups_with_deals_for_relevance_completion = generate_external_sensor_task(
    external_task_id=
    "uf_columnstore_v2_no_xd_level_rollups_cluster-with-relevance-cols_for_relevance_watch_task_uf-columnstore-v2_no_xd_level-id-combinations-avails-with-relevance-cols-for-relevance",
    external_dag_id="uf-columnstore-v2_no_xd_level-data-generator",
    task_id="wait_for_rollups_with_deals_for_relevance_completion",
    execution_delta=timedelta(hours=2)
)

wait_for_rollups_without_deals_for_relevance_completion = generate_external_sensor_task(
    external_task_id=
    "uf_columnstore_v2_no_xd_level_rollups_cluster-with-relevance-cols_watch_task_uf-columnstore-v2_no_xd_level-id-combinations-avails-with-relevance-cols",
    external_dag_id="uf-columnstore-v2_no_xd_level-data-generator",
    task_id="wait_for_rollups_without_deals_for_relevance_completion",
    execution_delta=timedelta(hours=2)
)

wait_for_rsm_data_generation = generate_external_sensor_task(
    external_dag_id="perf-automation-rsmv2-offline-score",
    external_task_id="final_dag_status",
    task_id="wait_for_rsm_data_generation",
    execution_delta=timedelta(days=1, hours=11, minutes=20),
)


def generate_relevance_avails_loading_jobs_for_clickhouse(date: str):
    # The order of the columns defined should match the order of the columns in the ClickHouse table schema.
    # Otherwise, the query will fail.
    clickhouse_columns = """
           RenderingContext,
           DeviceType,
           bitmapBuild(arrayFlatten(arrayMap(r -> arrayMap(x -> toUInt64(x + (r * 8)), bitPositionsToArray(reinterpretAsUInt64(substring(reverse(ifNull(InventoryChannels, '')), r + 1, 64)))), range(0, length(ifNull(InventoryChannels, '')), 8)))) AS InventoryChannel,
           CountryId,
           MetroId,
           RegionId,
           CityId,
           ZipId,
           PropertyId,
           bitmapBuild(arrayFlatten(arrayMap(r -> arrayMap(x -> toUInt64(x + (r * 8)), bitPositionsToArray(reinterpretAsUInt64(substring(reverse(ifNull(MarketplaceIds, '')), r + 1, 64)))), range(0, length(ifNull(MarketplaceIds, '')), 8)))) AS MarketplaceId,
           DeviceModelId,
           bitmapBuild(arrayMap(x->toUInt64(x), DealPositions)) AS DealId
           """

    clickhouse_columns_with_type = """
    AttributionId String,
    CombinationId UInt64,
    RenderingContext UInt32,
    DeviceType UInt32,
    CountryId UInt32,
    MetroId UInt32,
    RegionId UInt32,
    CityId UInt32,
    ZipId UInt32,
    PropertyId UInt64,
    InventoryChannels String,
    MarketplaceIds String,
    DeviceModelId UInt32,
    DealPositions Array(Int32),
    OneDayCount UInt64,
    ThreeDayCount UInt64,
    FiveDayCount UInt64,
    SevenDayCount UInt64,
    FourteenDayCount UInt64,
    ThirtyDayCount UInt64,
    Date Date
    """

    s3_directory = f's3://ttd-forecasting-useast/env=prod/relevance/rsmIdCombinationAvails/v=2/date={date}/part-*.parquet'
    table_type = TableType.RelevanceV2IdMappedAvails
    ch_loading_query = \
        f"""INSERT INTO ttd_forecast.RelevanceNoXdAvailsWithAttributionId_Distributed
            SELECT toUUID(AttributionId) AS AttributionId,
                    CombinationId,
                    {clickhouse_columns},
                    OneDayCount,
                    ThreeDayCount,
                    FiveDayCount,
                    SevenDayCount,
                    FourteenDayCount,
                    ThirtyDayCount,
                    Date
            FROM s3(
                S3Credentials,
                url='{s3_directory}',
                format='Parquet',
                structure='{clickhouse_columns_with_type}'
            )
            SETTINGS max_insert_threads=16, max_memory_usage=123000000000, min_insert_block_size_bytes=2562500000;
            """

    clickhouse_load_task = OpTask(
        task_id='clickhouse_relevance_avails_load_task',
        op=ClickHouseOperator(
            task_id="clickhouse_relevance_avails_load_task",
            query_id="relevance-avails-loading-query-from-airflow_{{ ti.dag_id }}-{{ ti.task_id }}",
            sql=ch_loading_query,
            clickhouse_conn_id="altinity_clickhouse_prd-cluster_connection",
        )
    )

    on_success_task = columnstore_sql_functions.mark_dataset_loaded(
        date=date, table_type=table_type, xd_vendor_ids=[XdLevel.v2_no_xd_level], isClickHouse=True
    )
    tasks = [clickhouse_load_task, on_success_task]
    task_group_name = "clickhouse_relevance_avails_loading_tasks"

    return ChainOfTasks(task_id=task_group_name, tasks=tasks).as_taskgroup(task_group_name)


def generate_relevance_audience_loading_jobs_for_clickhouse(date: str) -> ChainOfTasks:
    # The order of the columns defined should match the order of the columns in the ClickHouse table schema.
    # Otherwise, the query will fail.
    clickhouse_columns = """
           CombinationId,
           TargetingDataId,
           Date
    """

    clickhouse_columns_with_type = """
        CombinationId Int64,
        TargetingDataId Int64,
        Date Date
        """

    s3_directory = f's3://ttd-forecasting-useast/env=prod/relevance/rsmIdCombinationAudience/date={date}/part-*.parquet'
    table_type = TableType.RelevanceV2IdMappedAudience
    # TODO: At some point we need this queries to be env-aware. For now, just focusing on prod env
    ch_loading_query = \
        f"""INSERT INTO ttd_forecast.RelevanceNoXdAudienceWithAttributionId_Distributed
                SELECT CombinationId,
                    TargetingDataId,
                    Date
                FROM s3(
                    S3Credentials,
                    url='{s3_directory}',
                    format='Parquet',
                    structure='{clickhouse_columns_with_type}'
                    )
                SETTINGS max_insert_threads=16, max_memory_usage=123000000000, min_insert_block_size_bytes=2562500000;
            """
    clickhouse_load_task = OpTask(
        task_id='clickhouse_relevance_audience_load_task',
        op=ClickHouseOperator(
            task_id="clickhouse_relevance_audience_load_task",
            query_id="relevance-audience-loading-query-from-airflow_{{ ti.dag_id }}-{{ ti.task_id }}",
            sql=ch_loading_query,
            clickhouse_conn_id="altinity_clickhouse_prd-cluster_connection",
        )
    )

    on_success_task = columnstore_sql_functions.mark_dataset_loaded(
        date=date, table_type=table_type, xd_vendor_ids=[XdLevel.v2_no_xd_level], isClickHouse=True
    )
    tasks = [clickhouse_load_task, on_success_task]
    task_group_name = "clickhouse_relevance_audience_load_tasks"
    return ChainOfTasks(task_id=task_group_name, tasks=tasks).as_taskgroup(task_group_name)


def generate_relevance_avails_loading_jobs(date: str, columns: str) -> ChainOfTasks:
    loading_query = f"""COPY ttd_forecast.RelevanceNoXdAvailsWithAttributionId
                ( id_filler FILLER VARCHAR(36),
                    AttributionId AS id_filler::UUID,
                    CombinationId,
                    {columns}
                    OneDayCount,
                    ThreeDayCount,
                    FiveDayCount,
                    SevenDayCount,
                    FourteenDayCount,
                    ThirtyDayCount,
                    Date
                )"""

    directory_root = f's3://ttd-forecasting-useast/env=prod/relevance/rsmIdCombinationAvails/v=1/date={date}'
    table_type = TableType.RelevanceV2IdMappedAvails

    return columnstore_sql_functions.create_simple_load_task_group(
        loading_query=loading_query,
        directory_root=directory_root,
        direct_copy=False,
        task_id_base="relevance_avails",
        task_group_name="load_relevance_avails",
        on_load_complete=columnstore_sql_functions
        .mark_dataset_loaded(date=date, table_type=table_type, xd_vendor_ids=[XdLevel.v2_no_xd_level])
    )


def generate_relevance_audience_loading_jobs(date: str) -> ChainOfTasks:
    loading_query = """COPY ttd_forecast.RelevanceNoXdAudienceWithAttributionId
                ( CombinationId,
                    TargetingDataId,
                    Date
                )"""

    directory_root = f's3://ttd-forecasting-useast/env=prod/relevance/rsmIdCombinationAudience/date={date}'
    table_type = TableType.RelevanceV2IdMappedAudience

    return columnstore_sql_functions.create_simple_load_task_group(
        loading_query=loading_query,
        directory_root=directory_root,
        direct_copy=True,
        task_id_base="relevance_audience",
        task_group_name="load_relevance_audience",
        on_load_complete=columnstore_sql_functions
        .mark_dataset_loaded(date=date, table_type=table_type, xd_vendor_ids=[XdLevel.v2_no_xd_level])
    )


def refresh_datasets(op_task_id: str, bash_operator_id: str) -> OpTask:
    return OpTask(
        task_id=op_task_id,
        op=BashOperator(
            task_id=bash_operator_id,
            bash_command="curl -X GET https://haystack.gen.adsrvr.org/refresh",
            # Ensures that the step doesn't fail due to autoscaling killing the pod.
            executor_config=K8sExecutorConfig.watch_task()
        )
    )


columns = """RenderingContext,
                DeviceType,
                CountryId,
                MetroId,
                RegionId,
                CityId,
                ZipId,
                PropertyId,
                unpaddedInventoryChannel FILLER VARBINARY(4),
                InventoryChannel AS ttd_forecast.ToPaddedBinary(unpaddedInventoryChannel, 8),
                unpaddedMarketplaceId FILLER VARBINARY(100),
                MarketPlaceId AS ttd_forecast.ToPaddedBinary(unpaddedMarketplaceId, 200),
                DealPositions FILLER ARRAY[INT],
                DeviceMakeModel,"""

avails_loading_jobs = generate_relevance_avails_loading_jobs(date, columns)

audience_loading_jobs = generate_relevance_audience_loading_jobs(date)

validate_vector_values_loaded = generate_external_sensor_task(
    external_task_id="mark_VectorValueMappings_load_complete",
    external_dag_id="uf-columnstore-vector-value-dag",
    task_id="wait_for_vector_value_dag_load",
    execution_delta=timedelta(hours=2)
)

pin_mapped_datasets = columnstore_sql_functions.adjust_pinning_query(
    function="PinRelevanceTables",
    date=date,
    task_id="pin_relevance_data",
)

refresh_datasets_task = refresh_datasets("refresh_for_relevance_datasets", "trigger_curl_request_for_relevance_datasets")

id_filtering_cluster = generate_id_filtering_cluster(date)
rsm_avails_generation_cluster_for_vertica = generate_rsm_avails_generation_cluster(date, support_deals=False)
rsm_avails_generation_cluster_for_clickhouse = generate_rsm_avails_generation_cluster(date, support_deals=True)

rsm_audience_generation_cluster = generate_rsm_audience_generation_cluster(date)

clickhouse_relevance_avails_load_task = generate_relevance_avails_loading_jobs_for_clickhouse(date)
clickhouse_relevance_audience_load_task = generate_relevance_audience_loading_jobs_for_clickhouse(date)

dag >> wait_for_audience_dataset_completion
dag >> wait_for_rollups_without_deals_for_relevance_completion
dag >> wait_for_rsm_data_generation
dag >> wait_for_rollups_with_deals_for_relevance_completion

wait_for_audience_dataset_completion >> id_filtering_cluster
wait_for_rollups_without_deals_for_relevance_completion >> id_filtering_cluster
wait_for_rsm_data_generation >> id_filtering_cluster

wait_for_rollups_with_deals_for_relevance_completion >> rsm_avails_generation_cluster_for_clickhouse

id_filtering_cluster >> rsm_avails_generation_cluster_for_vertica
id_filtering_cluster >> rsm_avails_generation_cluster_for_clickhouse

rsm_avails_generation_cluster_for_vertica >> rsm_audience_generation_cluster

rsm_avails_generation_cluster_for_vertica >> avails_loading_jobs
rsm_audience_generation_cluster >> avails_loading_jobs
avails_loading_jobs >> audience_loading_jobs

rsm_audience_generation_cluster >> clickhouse_relevance_avails_load_task
rsm_avails_generation_cluster_for_clickhouse >> clickhouse_relevance_avails_load_task
clickhouse_relevance_avails_load_task >> clickhouse_relevance_audience_load_task

audience_loading_jobs >> validate_vector_values_loaded

# TODO: Uncomment following line when it's prod-ready
# clickhouse_relevance_audience_load_task >> validate_vector_values_loaded
validate_vector_values_loaded >> pin_mapped_datasets

pin_mapped_datasets >> refresh_datasets_task

# TODO: Uncomment when Airflow image issue is solved and the ClickHouse load jobs run successfully
# clickhouse_relevance_audience_load_task >> refresh_datasets_task

adag = dag.airflow_dag
