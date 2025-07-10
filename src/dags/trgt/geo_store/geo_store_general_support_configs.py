from dataclasses import dataclass

from ttd.ttdenv import TtdEnvFactory

geo_bucket = 'ttd-geo'

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    env = 'env=test'
    use_aerospike_test_cluster = "true"
else:
    env = 'env=prod'
    use_aerospike_test_cluster = "false"


@dataclass
class GeoStoreGeneralConfig:
    is_general_polygon: bool = True
    full_build_num_files: int = 10
    delta_build_num_files: int = 5
    geo_bucket: str = geo_bucket
    geo_store_generated_data_prefix: str = f"{env}/GeoStoreNg/GeoStoreGeneratedData"
    polygon_cell_mappings_path: str = "s3://ttd-geo/env=prod/GeoStoreNg/PolygonCellMappings"
    HDFSRootPath: str = "hdfs:///user/hadoop/output-temp-dir"
    delta_last_log: str = f"{env}/GeoStoreNg/DeltaProcessingLog/LastSuccessfulPaths.txt"
    latest_full_geo_tiles_log: str = f"{env}/GeoStoreNg/GeoStoreGeneratedData/LatestPrefix.txt"
    aerospike_daily_push_log: str = f"{env}/GeoStoreNg/DeltaProcessingLog/LastSuccessfulAerospikeDailyPush.txt"
    current_geo_targeting_data_path_key: str = "CurrentGeoTargetingDataPath"
    last_successful_geo_targeting_data_path_key: str = "LastSuccessfulGeoTargetingDataPath"
    current_targeting_data_geo_target_path_key: str = "CurrentTargetingDataGeoTargetPath"
    last_successful_targeting_data_geo_target_path_key: str = "LastSuccessfulTargetingDataGeoTargetPath"
    current_geo_target_path_key: str = "CurrentGeoTargetPath"
    geo_store_current_prefix_key: str = "CurrentGeoStorePrefix"
    geo_store_last_successful_prefix_key: str = "LastSuccessfulGeoStorePrefix"
    sensitive_places_source_path_key: str = "SensitivePlacesSourcePath"
    sensitive_places_source_output_path_key: str = "SensitivePlacesSourceOutputPath"
    built_sensitive_places_path_key: str = "BuiltSensitivePlacesPath"
    last_included_sensitive_places_path_key: str = "LastIncludedSensitivePlacesPath"
    need_build_monthly_sensitive_places_key: str = "NeedBuildMonthlySensitivePlaces"
    aerospike_daily_full_data_path_key: str = "AerospikeDailyFullDataPath"
    aerospike_hourly_full_data_path_key: str = "AerospikeHourlyFullDataPath"
    aerospike_hourly_delta_data_path_key: str = "AerospikeHourlyDeltaDataPath"
    cdc_bucket: str = "thetradedesk-useast-cdc-prod"
    targeting_data_geo_target_prefix: str = "ent-cdc/cdc-dbo-targetingdatageotarget-full-enc-json/type=full/v=1/"
    geo_target_prefix: str = "ent-cdc/cdc-dbo-geotarget-full-enc-json/type=full/v=1/"
    geo_targeting_data_bucket: str = geo_bucket
    geo_targeting_data_prefix: str = f"{env}/GeoStoreNg/GeoTargetingData/"
    sensitive_source_bucket: str = "thetradedesk-useast-data-import"
    sensitive_source_prefix: str = "factual/integrated_places/"
    sensitive_output_bucket: str = geo_bucket
    sensitive_output_prefix: str = f"{env}/GeoStoreNg/SensitivePlacesOutput/"
    political_polygon_source_path: str = f"s3://ttd-geo/{env}/GeoStoreNg/PoliticalPolygons/0"
    political_polygon_list_targeting_data_output_path: str = f"s3://ttd-geo/{env}/GeoStoreNg/PoliticalPolygons/PolygonTargetingOutput/0"
    restricted_state_shape_path_prefix: str = f"s3a://ttd-geo/{env}/GeoStoreNg/MinimumAreaCells"
    small_geo_targeting_data_id_sub_folder: str = "SmallGeo/SmallGeoTargetingDataId"
    standardized_partitioned_cell_to_targets_sub_folder: str = "StandardizedPartitionedCellToTargets"
    standardized_cell_to_targets_before_validation_sub_folder: str = "StandardizedCellToTargetsBeforeValidation"
    assembled_cell_data_sub_folder: str = "AssembledCells"
    custom_polygon_source_path: str = f"s3://ttd-geo/{env}/GeoStorePolygons/platform"
    use_custom_polygon_mock_data: str = str(env == 'env=test').lower()
    sharded_cell_data_sub_folder: str = "ShardedCells"
    geotile_size_buffer_in_byte: int = 8 * 1024
    geotile_partial_match_limit: int = 2000
    use_aerospike_test_cluster: str = use_aerospike_test_cluster


@dataclass
class GeoStoreGeneralFullNotPushingConfig(GeoStoreGeneralConfig):
    is_full_build: str = "true"
    geo_store_diff_cache_bucket: str = geo_bucket
    geo_store_diff_cache_v4_prefix: str = f"{env}/GeoStoreNg/diffcache4"
    forced_trunk_levels_path: str = "s3://ttd-geo/env=prod/GeoStoreNg/GeoStoreForcedTrunkLevels"


@dataclass
class GeoStoreGeneralFullPushingConfig(GeoStoreGeneralConfig):
    is_full_build: str = "true"
    geo_store_diff_cache_bucket: str = "thetradedesk-useast-geostore"
    geo_store_diff_cache_v4_prefix: str = f"{env}/diffcache4"
    forced_trunk_levels_path: str = "s3://ttd-geo/GeoStoreNg/GeoStoreForcedTrunkLevels"


@dataclass
class GeoStoreGeneralDeltaNotPushingConfig(GeoStoreGeneralConfig):
    is_full_build: str = "false"
    geo_store_diff_cache_bucket: str = geo_bucket
    geo_store_diff_cache_v4_prefix: str = f"{env}/GeoStoreNg/diffcache4"
    forced_trunk_levels_path: str = "s3://ttd-geo/env=prod/GeoStoreNg/GeoStoreForcedTrunkLevels"


@dataclass
class GeoStoreGeneralDeltaPushingConfig(GeoStoreGeneralConfig):
    is_full_build: str = "false"
    geo_store_diff_cache_bucket: str = "thetradedesk-useast-geostore"
    geo_store_diff_cache_v4_prefix: str = f"{env}/diffcache4"
    forced_trunk_levels_path: str = "s3://ttd-geo/GeoStoreNg/GeoStoreForcedTrunkLevels"
