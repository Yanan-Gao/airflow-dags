from dataclasses import dataclass


@dataclass
class GeoStoreFullNotPushingConfig:
    env: str = "env=prod"
    is_full_build: str = "true"
    job_jar: str = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar"
    full_build_num_files: int = 10
    delta_build_num_files: int = 5
    geo_bucket: str = "ttd-geo"
    geo_store_diff_cache_bucket: str = "ttd-geo"
    geo_store_diff_cache_prefix: str = f"{env}/GeoStoreNg/diffcache2"
    geo_store_diff_cache_v3_prefix: str = f"{env}/GeoStoreNg/diffcache3"
    geo_store_generated_data_prefix: str = f"{env}/GeoStoreNg/GeoStoreGeneratedData"
    forced_trunk_levels_path: str = "s3://ttd-geo/env=prod/GeoStoreNg/GeoStoreForcedTrunkLevels"
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

    def __init__(self, **overrides):
        for key, value in self.__class__.__dict__.items():
            if not key.startswith('__'):  # Exclude special methods/attributes
                setattr(self, key, value)

        for key, value in overrides.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"Invalid configuration key: {key}")

    # Serialization methods
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state


@dataclass
class GeoStoreFullPushingConfig:
    env: str = "env=prod"
    is_full_build: str = "true"
    job_jar: str = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar"
    full_build_num_files: int = 10
    delta_build_num_files: int = 5
    geo_bucket: str = "ttd-geo"
    geo_store_diff_cache_bucket: str = "thetradedesk-useast-geostore"
    geo_store_diff_cache_prefix: str = "diffcache2"
    geo_store_diff_cache_v3_prefix: str = "diffcache3"
    geo_store_generated_data_prefix: str = f"{env}/GeoStoreNg/GeoStoreGeneratedData"
    forced_trunk_levels_path: str = "s3://ttd-geo/GeoStoreNg/GeoStoreForcedTrunkLevels"
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

    def __init__(self, **overrides):
        for key, value in self.__class__.__dict__.items():
            if not key.startswith('__'):  # Exclude special methods/attributes
                setattr(self, key, value)

        for key, value in overrides.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"Invalid configuration key: {key}")

    # Serialization methods
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state


@dataclass
class GeoStoreDeltaNotPushingConfig:
    env: str = "env=prod"
    is_full_build: str = "false"
    job_jar: str = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar"
    full_build_num_files: int = 10
    delta_build_num_files: int = 5
    geo_bucket: str = "ttd-geo"
    geo_store_diff_cache_bucket: str = "ttd-geo"
    geo_store_diff_cache_prefix: str = f"{env}/GeoStoreNg/diffcache2"
    geo_store_diff_cache_v3_prefix: str = f"{env}/GeoStoreNg/diffcache3"
    geo_store_generated_data_prefix: str = f"{env}/GeoStoreNg/GeoStoreGeneratedData"
    forced_trunk_levels_path: str = "s3://ttd-geo/env=prod/GeoStoreNg/GeoStoreForcedTrunkLevels"
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

    def __init__(self, **overrides):
        for key, value in self.__class__.__dict__.items():
            if not key.startswith('__'):  # Exclude special methods/attributes
                setattr(self, key, value)

        for key, value in overrides.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"Invalid configuration key: {key}")

    # Serialization methods
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state


@dataclass
class GeoStoreDeltaPushingConfig:
    env: str = "env=prod"
    is_full_build: str = "false"
    job_jar: str = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar"
    full_build_num_files: int = 10
    delta_build_num_files: int = 5
    geo_bucket: str = "ttd-geo"
    geo_store_diff_cache_bucket: str = "thetradedesk-useast-geostore"
    geo_store_diff_cache_prefix: str = "diffcache2"
    geo_store_diff_cache_v3_prefix: str = "diffcache3"
    geo_store_generated_data_prefix: str = f"{env}/GeoStoreNg/GeoStoreGeneratedData"
    forced_trunk_levels_path: str = "s3://ttd-geo/GeoStoreNg/GeoStoreForcedTrunkLevels"
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


@dataclass
class GeoStoreStatsConfig:
    job_jar: str = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar"

    def __init__(self, **overrides):
        for key, value in self.__class__.__dict__.items():
            if not key.startswith('__'):  # Exclude special methods/attributes
                setattr(self, key, value)

        for key, value in overrides.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"Invalid configuration key: {key}")

    # Serialization methods
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state
