from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

from dags.forecast.columnstore.columnstore_common_setup import (
    ColumnStoreEmrSetup,
    ColumnStoreDAGSetup,
)
from dags.forecast.columnstore.columnstore_sql import ColumnStoreFunctions
from dags.forecast.columnstore.enums.table_type import TableType
from dags.forecast.columnstore.enums.xd_level import XdLevel, is_v2_xd_level, is_s3_migrated, get_xd_level_name, \
    get_mapped_pinning_function
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.compute_optimized.c6g import C6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask

columnstore_sql_functions = ColumnStoreFunctions()
emr_steps_common_parameters = ColumnStoreEmrSetup()
columnstore_dag_common_parameters = ColumnStoreDAGSetup()


def uses_uuids(xd_level: XdLevel) -> bool:
    match xd_level:
        case XdLevel.no_xd_level:
            return True
        case XdLevel.adbrain_person_level:
            return False
        case XdLevel.adbrain_household_level:
            return True
        case XdLevel.v2_no_xd_level:
            return True
        case XdLevel.opengraph_person_level:
            return False
        case XdLevel.opengraph_household_level:
            return True
        case _:
            raise NotImplementedError


def generate_id_combination_avails_loading_jobs(
    xd_level: XdLevel, date: str, sample_rate: int, columns: str, use_relevance_columns: bool = False
) -> ChainOfTasks:
    is_uuid = uses_uuids(xd_level)
    xd_level_table_name = get_xd_level_name(xd_level)
    migrated = is_s3_migrated(xd_level)

    uuid_loading_query = f"""COPY ttd_forecast.{xd_level_table_name}AvailsWithAttributionId_SampleRate_{sample_rate}
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

    non_uuid_loading_query = f"""COPY ttd_forecast.{xd_level_table_name}AvailsWithAttributionId_SampleRate_{sample_rate}
                ( AttributionId,
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

    # Logic duplicated from elt-based-forecasts.
    version = "3" if use_relevance_columns else "2"
    directory_root = f"s3://ttd-forecasting-useast/env=prod/columnstore/rolledUpXDAvails/v={version}/xdvendorid={xd_level.value}/identityGraph=openGraphIav2/sampleRate={sample_rate}/rollupLength=30/date={date}" \
        if migrated else \
        f"s3://ttd-identity/datapipeline/prod/rolledUpXDAvails/v={version}/xdvendorid={xd_level.value}/sampleRate={sample_rate}/rollupLength=30/date={date}"
    table_type = TableType.V2IdMappedAvails if is_v2_xd_level(xd_level) else TableType.IdMappedAvails

    return columnstore_sql_functions.create_uuid_variable_load_task_group(
        is_uuid=is_uuid,
        uuid_loading_query=uuid_loading_query,
        non_uuid_loading_query=non_uuid_loading_query,
        directory_root=directory_root,
        direct_copy_without_uuid=True,
        task_id_base="combination_avails",
        task_group_name="id_combination_avails_loading_group",
        on_load_complete=columnstore_sql_functions.mark_dataset_loaded(date, [xd_level], table_type)
    )


def generate_id_combination_audience_loading_jobs(xd_level: XdLevel, date: str, sample_rate) -> ChainOfTasks:
    xd_level_table_name = get_xd_level_name(xd_level)
    loading_query = f"""COPY ttd_forecast.{xd_level_table_name}AudienceWithAttributionId_SampleRate_{sample_rate}
                ( CombinationId,
                    TargetingDataId,
                    Date
                )"""

    directory_root = f"s3://ttd-identity/datapipeline/prod/explodedseeninbiddingrollups/v=2/xdvendorid={xd_level.value}/sampleRate={sample_rate}/length=7/date={date}"
    table_type = TableType.V2IdMappedAudience if is_v2_xd_level(xd_level) else TableType.IdMappedAudience

    return columnstore_sql_functions.create_simple_load_task_group(
        loading_query=loading_query,
        directory_root=directory_root,
        direct_copy=True,
        task_id_base="combination_audience",
        task_group_name="id_combination_audience_loading_group",
        on_load_complete=columnstore_sql_functions.mark_dataset_loaded(date, [xd_level], table_type),
    )


def create_avails_aggregation_job(xd_level: XdLevel, date: str) -> EmrClusterTask:
    xd_level_class_name = get_xd_level_name(xd_level)

    aggregation_step_name = (f"uf-columnstore-{xd_level.name}-avails-aggregation-for-rollup")
    cluster_name = (f"uf_columnstore_{xd_level.name}_avails_aggregation_cluster")
    aggregation_job_class = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.avails.aggregategenerators.GenerateAggregateAvailsData"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=2880,
    )

    avails_transformation_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    daily_aggregation_step = EmrJobTask(
        name=aggregation_step_name,
        class_name=aggregation_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
            ("writeFileCount", 2880),
            ("xdLevel", xd_level_class_name),
        ],
        additional_args_option_pairs_list=[("conf", "spark.sql.shuffle.partitions=5760")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=4),
    )

    avails_transformation_cluster.add_parallel_body_task(daily_aggregation_step)

    return avails_transformation_cluster


def create_sib_explosion_job(xd_level: XdLevel, date: str) -> EmrClusterTask:
    xd_level_class_name = get_xd_level_name(xd_level)

    job_name = f"uf-columnstore-{xd_level.name}-sib-inverter"
    cluster_name = f"uf_columnstore_{xd_level.name}_sib_inversion_cluster"
    job_class = f"com.thetradedesk.etlforecastjobs.preprocessing.columnstore.audience.Generate{xd_level_class_name}LevelSIBExplodedData"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=960,
    )

    sib_explosion_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    step = EmrJobTask(
        name=job_name,
        class_name=job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
        ],
        additional_args_option_pairs_list=[("conf", "spark.sql.shuffle.partitions=3000")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=4),
    )

    sib_explosion_cluster.add_parallel_body_task(step)

    return sib_explosion_cluster


def validate_upstream_data_generated():
    return OpTask(op=EmptyOperator(
        task_id="check_data_generation_succeeds",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    ))


def create_rollups_for_relevance(
    xd_level: XdLevel, date: str, with_relevance_cols: bool = True, number_of_partitions: int = 60000
) -> EmrClusterTask:

    xd_level_class_name = get_xd_level_name(xd_level)
    with_relevance_cols_suffix = "-with-relevance-cols" if with_relevance_cols else ""

    cluster_name = f"uf_columnstore_{xd_level.name}_rollups_cluster{with_relevance_cols_suffix}_for_relevance"
    id_combination_rollup_avails_for_relevance_job_name = f"uf-columnstore-{xd_level.name}-id-combinations-avails{with_relevance_cols_suffix}-for-relevance"
    id_combination_rollup_avails_for_relevance_job_class = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.avails.rollupgenerators.GenerateRollupData"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[R6g.r6g_16xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R6g.r6g_8xlarge().with_fleet_weighted_capacity(256).with_ebs_size_gb(1024),
            R6g.r6g_12xlarge().with_fleet_weighted_capacity(384).with_ebs_size_gb(1536),
            R6g.r6g_16xlarge().with_fleet_weighted_capacity(512).with_ebs_size_gb(2048),
        ],
        on_demand_weighted_capacity=60_000,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=960,
    )

    id_combination_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    id_combination_rollup_avails_step_for_relevance = EmrJobTask(
        name=id_combination_rollup_avails_for_relevance_job_name,
        class_name=id_combination_rollup_avails_for_relevance_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
            ("withIdCombination", "true"),
            ("ttd.ds.IdCombinationsMappingDataset.isInChain", "true"),
            ("useNewColumns", "true" if with_relevance_cols else "false"),
            ("xdLevel", xd_level_class_name),
            ("supportDeals", "true"),
        ],
        additional_args_option_pairs_list=[("conf", f"spark.sql.shuffle.partitions={number_of_partitions}")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=10) if with_relevance_cols else timedelta(hours=1),
    )

    id_combination_cluster.add_parallel_body_task(id_combination_rollup_avails_step_for_relevance)

    return id_combination_cluster


def create_rollups(xd_level: XdLevel, date: str, with_relevance_cols: bool = False, number_of_partitions: int = 60000) -> EmrClusterTask:
    xd_level_class_name = get_xd_level_name(xd_level)
    with_relevance_cols_suffix = "-with-relevance-cols" if with_relevance_cols else ""

    cluster_name = f"uf_columnstore_{xd_level.name}_rollups_cluster{with_relevance_cols_suffix}"
    id_combination_rollup_avails_job_name = f"uf-columnstore-{xd_level.name}-id-combinations-avails{with_relevance_cols_suffix}"
    id_combination_rollup_avails_job_class = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.avails.rollupgenerators.GenerateRollupData"
    id_combination_audience_job_name = f"uf-columnstore-{xd_level.name}-id-combinations-audience"
    id_combination_audience_job_class = f"com.thetradedesk.etlforecastjobs.preprocessing.columnstore.audience.Generate{xd_level_class_name}LevelSIBCombinationData"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[R6g.r6g_16xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R6g.r6g_8xlarge().with_fleet_weighted_capacity(256),
            R6g.r6g_12xlarge().with_fleet_weighted_capacity(384),
            R6g.r6g_16xlarge().with_fleet_weighted_capacity(512),
        ],
        on_demand_weighted_capacity=60_000,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=960,
    )

    id_combination_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    id_combination_rollup_avails_step = EmrJobTask(
        name=id_combination_rollup_avails_job_name,
        class_name=id_combination_rollup_avails_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
            ("withIdCombination", "true"),
            ("ttd.ds.IdCombinationsMappingDataset.isInChain", "true"),
            ("useNewColumns", "true" if with_relevance_cols else "false"),
            ("xdLevel", xd_level_class_name),
            ("supportDeals", "false"),
        ],
        additional_args_option_pairs_list=[("conf", f"spark.sql.shuffle.partitions={number_of_partitions}")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=12) if with_relevance_cols else timedelta(hours=1),
    )

    id_combination_audience_step = EmrJobTask(
        name=id_combination_audience_job_name,
        class_name=id_combination_audience_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
            ("ttd.ds.IdCombinationsMappingDataset.isInChain", "true"),
        ],
        additional_args_option_pairs_list=[("conf", f"spark.sql.shuffle.partitions={number_of_partitions}")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=2),
    )

    id_combination_cluster.add_sequential_body_task(id_combination_rollup_avails_step)
    id_combination_cluster.add_sequential_body_task(id_combination_audience_step)

    return id_combination_cluster


def create_transformed_aggregates(
    xd_level: XdLevel, date: str, with_relevance_cols: bool = False, number_of_partitions: int = 21000
) -> EmrClusterTask:

    xd_level_class_name = get_xd_level_name(xd_level)
    cluster_name = f"uf_columnstore_{xd_level.name}_transformed_aggregates"
    create_transformed_aggregates_job_name = f"uf-columnstore-{xd_level.name}-create-transformed-aggregates"
    create_transformed_aggregates_job_class = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.avails.aggregategenerators.GenerateTransformedAggregates"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[M6g.m6g_8xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            C6g.c6g_2xlarge().with_fleet_weighted_capacity(64),
            C6g.c6g_4xlarge().with_fleet_weighted_capacity(128),
            C6g.c6g_8xlarge().with_fleet_weighted_capacity(256),
            C6g.c6g_12xlarge().with_fleet_weighted_capacity(384),
        ],
        on_demand_weighted_capacity=40_000,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=960,
    )

    add_transformed_aggregates_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    spark_options_list = [("conf", f"spark.sql.shuffle.partitions={number_of_partitions}"), ("conf", "spark.driver.memory=8G"),
                          ("conf", "spark.driver.maxResultSize=20G")]
    create_transformed_aggregates_step = EmrJobTask(
        name=create_transformed_aggregates_job_name,
        class_name=create_transformed_aggregates_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
            ("ttd.ds.IdCombinationsMappingDataset.isInChain", "true"),
            ("xdLevel", xd_level_class_name),
        ],
        additional_args_option_pairs_list=spark_options_list,
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=9) if with_relevance_cols else timedelta(hours=1),
    )

    add_transformed_aggregates_cluster.add_parallel_body_task(create_transformed_aggregates_step)

    return add_transformed_aggregates_cluster


def create_aggregates_with_marketplaces(
    xd_level: XdLevel, date: str, with_relevance_cols: bool = False, number_of_partitions: int = 21000
) -> EmrClusterTask:

    xd_level_class_name = get_xd_level_name(xd_level)
    cluster_name = f"uf_columnstore_{xd_level.name}_aggregates_with_marketplaces_cluster"
    id_combination_add_marketplaces_to_agg_job_name = f"uf-columnstore-{xd_level.name}-add-marketplaces-to-aggregated-avails"
    id_combination_add_marketplaces_to_agg_job_class = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.avails.aggregategenerators.GenerateAggregatedAvailsWithMarketplaces"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[M6g.m6g_8xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            C6g.c6g_2xlarge().with_fleet_weighted_capacity(64),
            C6g.c6g_4xlarge().with_fleet_weighted_capacity(128),
            C6g.c6g_8xlarge().with_fleet_weighted_capacity(256),
            C6g.c6g_12xlarge().with_fleet_weighted_capacity(384),
        ],
        on_demand_weighted_capacity=80_000,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=960,
    )

    add_marketplaces_to_aggregates_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    spark_options_list = [("conf", f"spark.sql.shuffle.partitions={number_of_partitions}"), ("conf", "spark.driver.memory=8G"),
                          ("conf", "spark.driver.maxResultSize=20G")]
    id_combination_add_marketplaces_to_agg_step = EmrJobTask(
        name=id_combination_add_marketplaces_to_agg_job_name,
        class_name=id_combination_add_marketplaces_to_agg_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
            ("ttd.ds.IdCombinationsMappingDataset.isInChain", "true"),
            ("xdLevel", xd_level_class_name),
        ],
        additional_args_option_pairs_list=spark_options_list,
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=12) if with_relevance_cols else timedelta(hours=1),
    )

    add_marketplaces_to_aggregates_cluster.add_parallel_body_task(id_combination_add_marketplaces_to_agg_step)

    return add_marketplaces_to_aggregates_cluster


def create_id_combinations_datasets(
    xd_level: XdLevel, date: str, with_relevance_cols: bool = False, number_of_partitions: int = 3000
) -> EmrClusterTask:
    xd_level_class_name = get_xd_level_name(xd_level)

    with_relevance_cols_suffix = "-with-relevance-cols" if with_relevance_cols else ""

    cluster_name = f"uf_columnstore_{xd_level.name}_id_combinations_cluster{with_relevance_cols_suffix}"
    id_combination_job_name = f"uf-columnstore-{xd_level.name}-id-combinations"
    id_combination_job_class = f"com.thetradedesk.etlforecastjobs.preprocessing.columnstore.avails.idcombinationgenerators.Generate{xd_level_class_name}LevelIdCombinationData"

    master_fleet_instance_configs = EmrFleetInstanceTypes(
        instance_types=[R6g.r6g_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R6g.r6g_4xlarge().with_fleet_weighted_capacity(64),
            R6g.r6g_8xlarge().with_fleet_weighted_capacity(128),
            R6g.r6g_12xlarge().with_fleet_weighted_capacity(176),
            R6g.r6g_16xlarge().with_fleet_weighted_capacity(256),
        ],
        on_demand_weighted_capacity=12_000,
    ) if with_relevance_cols else EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_4xlarge().with_fleet_weighted_capacity(16),
            C5.c5_9xlarge().with_fleet_weighted_capacity(36),
            C5.c5_12xlarge().with_fleet_weighted_capacity(48),
            C5.c5_18xlarge().with_fleet_weighted_capacity(72),
            C5.c5_24xlarge().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=960,
    )

    id_combination_cluster = EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        cluster_tags=emr_steps_common_parameters.cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=emr_steps_common_parameters.emr_release_label,
        log_uri=emr_steps_common_parameters.log_uri,
        use_on_demand_on_timeout=True,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True
    )

    id_combination_step = EmrJobTask(
        name=id_combination_job_name,
        class_name=id_combination_job_class,
        eldorado_config_option_pairs_list=[
            ("date", date),
            ("enableLogging", "true"),
        ],
        additional_args_option_pairs_list=[("conf", f"spark.sql.shuffle.partitions={number_of_partitions}")],
        executable_path=emr_steps_common_parameters.job_jar,
        timeout_timedelta=timedelta(hours=1),
    )

    id_combination_cluster.add_parallel_body_task(id_combination_step)

    return id_combination_cluster


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


def create_xd_dag(xd_level: XdLevel) -> TtdDag:
    job_name = f"uf-columnstore-{xd_level.name}-data-generator"

    job_start_date = datetime(year=2024, month=8, day=13, hour=10, minute=0)
    # Every day at 10 o'clock
    job_schedule_interval = "0 10 * * *"
    active_running_jobs = 1

    run_date = """{{ data_interval_start.subtract(days=1).to_date_string() }}"""
    run_datetime = """{{ data_interval_start.subtract(days=1).to_datetime_string() }}"""

    columnstore_data_generator_dag = TtdDag(
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
    )

    def validate_vector_values_loaded(task_id: str):
        return OpTask(
            op=ExternalTaskSensor(
                task_id=task_id,
                external_dag_id="uf-columnstore-vector-value-dag",
                # we just need this one task to be complete
                external_task_id="mark_VectorValueMappings_load_complete",
                allowed_states=["success"],
                check_existence=False,
                poke_interval=30,
                timeout=60,
                mode="reschedule",  # release the worker slot between pokes
                dag=columnstore_data_generator_dag.airflow_dag
            )
        )

    sib_explosion_job = create_sib_explosion_job(xd_level, run_date)
    avails_aggregation_job = create_avails_aggregation_job(xd_level, run_date)

    if xd_level == XdLevel.opengraph_household_level:
        wait_for_input_data = DatasetCheckSensor(
            dag=columnstore_data_generator_dag.airflow_dag,
            task_id="wait-for-data-agg",
            ds_date=run_datetime,
            poke_interval=60 * 10,
            datasets=[AvailsDatasources.household_sampled_high_sample_avails_openGraphIav2.with_check_type("day")],
        )
        wait_for_input_data >> avails_aggregation_job.first_airflow_op()
    elif xd_level == XdLevel.opengraph_person_level:
        wait_for_input_data = DatasetCheckSensor(
            dag=columnstore_data_generator_dag.airflow_dag,
            task_id="wait-for-data-agg",
            ds_date=run_datetime,
            poke_interval=60 * 10,
            datasets=[AvailsDatasources.person_sampled_avails_openGraphIav2.with_check_type("day")],
        )
        wait_for_input_data >> avails_aggregation_job.first_airflow_op()

    columnstore_data_generator_dag >> avails_aggregation_job
    columnstore_data_generator_dag >> sib_explosion_job

    all_unmapped_data_generated = validate_upstream_data_generated()

    sib_explosion_job >> all_unmapped_data_generated
    avails_aggregation_job >> all_unmapped_data_generated

    # Adding steps for id-mapped data generation and loading
    generate_id_combination_datasets = create_id_combinations_datasets(xd_level, run_date, True, 9000)

    generate_aggregates_with_marketplaces = create_aggregates_with_marketplaces(xd_level, run_date, True, 21000)

    transformed_aggregates = create_transformed_aggregates(xd_level, run_date, True, 21000)

    # RollUps have different steps as it divides-and-processess the data.
    # In the latter stages of union'ing, aiming for partitons around 600Mb.
    generate_rollups = create_rollups(xd_level, run_date, True, 60000)

    # Order in which columns are defined here matter, they should match ThirtyDayRolledUpAvailsWithIdCombination
    # in etl-based-forecasts
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

    id_mapped_avails_loading_job = generate_id_combination_avails_loading_jobs(
        xd_level, run_date, sample_rate=60, columns=columns, use_relevance_columns=True
    )
    id_mapped_audience_loading_job = generate_id_combination_audience_loading_jobs(xd_level, run_date, sample_rate=60)

    # Naming's not great here, but this should be a temporary duplication of the task, and the names should be roughly
    # the same. The two tasks are doing exactly the same thing, we just don't want to block the refresh on the prod
    # branch by using the same OpTask
    validate_all_mapped_data_loaded = validate_vector_values_loaded("wait_for_vector_value_dag_load")

    pin_mapped_datasets = columnstore_sql_functions.adjust_pinning_query(
        function=get_mapped_pinning_function(xd_level),
        date=run_date,
        task_id=f"pin_mapped_{xd_level.name}",
    )

    all_unmapped_data_generated >> generate_id_combination_datasets

    generate_id_combination_datasets >> generate_aggregates_with_marketplaces

    generate_aggregates_with_marketplaces >> transformed_aggregates

    transformed_aggregates >> generate_rollups

    if (xd_level is XdLevel.v2_no_xd_level):
        generate_rollups_for_relevance = create_rollups_for_relevance(xd_level, run_date, True, 60000)
        transformed_aggregates >> generate_rollups_for_relevance

    generate_rollups >> validate_all_mapped_data_loaded

    generate_id_combination_datasets >> id_mapped_avails_loading_job

    generate_rollups >> id_mapped_avails_loading_job

    id_mapped_avails_loading_job >> id_mapped_audience_loading_job

    id_mapped_audience_loading_job >> validate_all_mapped_data_loaded

    validate_all_mapped_data_loaded >> pin_mapped_datasets

    pin_mapped_datasets >> refresh_datasets("refresh_for_mapped_datasets", "trigger_curl_request_for_mapped_datasets")

    return columnstore_data_generator_dag


for xd_level in XdLevel:
    if not is_v2_xd_level(xd_level):
        continue
    dag = create_xd_dag(xd_level)
    globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
