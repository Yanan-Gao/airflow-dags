from ttd.slack.slack_groups import targeting
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils, job_jar_prefix, geostore_jar_name_key, default_jar_name
import copy


class GeoStoreClusters:

    def __init__(self, job_name, push_values_to_xcom_task_id, config):
        self.job_name = job_name
        self.push_values_to_xcom_task_id = push_values_to_xcom_task_id
        self.config = config

    partitioning_cluster_num_cores = 32
    expanding_cluster_num_cores = 32
    converting_cluster_num_cores = 32

    def get_value(self, key: str) -> str:
        return (
            f"{{{{ "
            f'task_instance.xcom_pull(dag_id="{self.job_name}", '
            f'task_ids="{self.push_values_to_xcom_task_id}", '
            f'key="{key}") '
            f"}}}}"
        )

    def get_job_jar(self, use_xcom_value: bool = True):
        geo_store_jar_name_xcom = self.get_value(geostore_jar_name_key)
        return f'{job_jar_prefix}/{geo_store_jar_name_xcom if use_xcom_value else default_jar_name}'

    cluster_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    )

    def cluster_core_fleet_instance_type_configs(self, on_demand_weighted_capacity):
        return EmrFleetInstanceTypes(instance_types=[R5d.r5d_8xlarge()], on_demand_weighted_capacity=on_demand_weighted_capacity)

    aerospike_cluster_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    )

    aerospike_cluster_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=8
    )

    java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

    def get_additional_args(self, core_nums):
        return [
            ("conf", "spark.driver.memory=64g"),
            ("conf", "spark.driver.cores=8"),
            ("conf", "spark.executor.cores=15"),
            ("conf", "spark.executor.memory=100g"),
            ("conf", "spark.executor.memoryOverhead=10g"),
            ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
            ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
            ("conf", f"spark.sql.shuffle.partitions={core_nums * 2}"),
            ("conf", f"spark.default.parallelism={core_nums * 2}"),
            ("conf", "spark.memory.fraction=0.9"),
            ('conf', 'spark.kryoserializer.buffer.max=512m'),
        ]

    def _create_cluster(self, cluster_label, master_fleet_instance_type_configs, core_fleet_instance_type_configs, aws_region, kwargs):
        return EmrClusterTask(
            name=cluster_label,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            cluster_tags={"Team": targeting.jira_team},
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
            region_name=aws_region,
            **kwargs
        )

    def get_geo_store_cluster(self, cluster_label, core_nums) -> EmrClusterTask:
        return self._create_cluster(
            cluster_label, self.cluster_master_fleet_instance_type_configs, self.cluster_core_fleet_instance_type_configs(core_nums),
            "us-east-1", GeoStoreUtils.cluster_kwargs_by_region["us-east-1"]
        )

    def get_full_job_expanding_clusters(self) -> EmrClusterTask:
        return self._create_cluster(
            'build_geo_store', self.cluster_master_fleet_instance_type_configs, self.cluster_core_fleet_instance_type_configs, "us-east-1",
            GeoStoreUtils.cluster_kwargs_by_region["us-east-1"]
        )

    def get_geo_targets_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="get_geo_target",
            class_name="com.thetradedesk.jobs.delta.GetGeoTargets",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build),
             ("currentTargetingDataPath", self.get_value(self.config.current_geo_targeting_data_path_key)),
             ("currentTargetingDataGeoTargetPath", self.get_value(self.config.current_targeting_data_geo_target_path_key)),
             ("currentGeoTargetPath", self.get_value(self.config.current_geo_target_path_key)),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("lastSuccessfulTargetingDataPath", self.get_value(self.config.last_successful_geo_targeting_data_path_key)),
             ("lastSuccessfulTargetingDataGeoTargetPath", self.get_value(self.config.last_successful_targeting_data_geo_target_path_key)),
             ("geoStoreLastSuccessfulPrefix", self.get_value(self.config.geo_store_last_successful_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_all_geo_targets_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="get_all_geo_target",
            class_name="com.thetradedesk.jobs.general.GeoTargetFetcher",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build),
             ("currentTargetingDataPath", self.get_value(self.config.current_geo_targeting_data_path_key)),
             ("currentTargetingDataGeoTargetPath", self.get_value(self.config.current_targeting_data_geo_target_path_key)),
             ("currentGeoTargetPath", self.get_value(self.config.current_geo_target_path_key)),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def remove_small_geo_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="remove_small_geo",
            class_name="com.thetradedesk.jobs.delta.RemoveSmallGeos",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("stateShapeCellLevel", 17), ("usStateMinimumRadiusInMeters", 914.4), ("defaultMinimumRadiusInMeters", 100),
             ("isFullBuild", self.config.is_full_build), ("segmentShapePathPrefix", "s3a://ttd-geo/GeoStoreNg/ConfigMinimumAreaShapes/"),
             ("segmentShapeFiles", "washington,newyork,connecticut,nevada"),
             ("geoStorePolygonCellMappingsPath", self.config.polygon_cell_mappings_path),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("geoStoreLastSuccessfulPrefix", self.get_value(self.config.geo_store_last_successful_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def remove_small_circles_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="remove_small_cicles",
            class_name="com.thetradedesk.jobs.general.SmallCircleRemover",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("stateShapeCellLevel", 17), ("usStateMinimumRadiusInMeters", 914.4), ("defaultMinimumRadiusInMeters", 100),
             ("isFullBuild", self.config.is_full_build), ("segmentShapePathPrefix", "s3a://ttd-geo/GeoStoreNg/ConfigMinimumAreaShapes/"),
             ("segmentShapeFiles", "washington,newyork,connecticut,nevada"),
             ("geoStorePolygonCellMappingsPath", self.config.polygon_cell_mappings_path),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)), ("geoStoreLastSuccessfulPrefix", "NA")],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def load_custom_polygon_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="load_custom_polygon",
            class_name="com.thetradedesk.jobs.general.CustomPolygonLoader",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("customPolygonSourcePath", self.config.custom_polygon_source_path),
             ("useCustomPolygonMockData", self.config.use_custom_polygon_mock_data),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("processedDataPath", self.config.processed_data_path)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_process_custom_polygons_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="process_custom_polygons_into_standard",
            class_name="com.thetradedesk.jobs.general.CustomPolygonTargetProcessor",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("fullBuildNumFiles", self.config.full_build_num_files),
             ("deltaBuildNumFiles", self.config.delta_build_num_files),
             ("StandardizedCellToTargetsBeforeValidationSubPath", self.config.standardized_cell_to_targets_before_validation_sub_folder),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def remove_small_polygon_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="remove_small_polygons",
            class_name="com.thetradedesk.jobs.general.SmallCustomPolygonRemover",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("fullBuildNumFiles", self.config.full_build_num_files),
             ("deltaBuildNumFiles", self.config.delta_build_num_files), ("restrictedStateMinimumAreaInSquareMeters", 2626770),
             ("defaultMinimumAreaInSquareMeters", 31416), ("restrictedStates", "Washington,NewYork,Connecticut,Nevada"),
             ("restrictedStateShapePathPrefix", self.config.restricted_state_shape_path_prefix),
             ("smallGeoTargetingDataIdSubFolder", self.config.small_geo_targeting_data_id_sub_folder),
             ("StandardizedCellToTargetsBeforeValidationSubPath", self.config.standardized_cell_to_targets_before_validation_sub_folder),
             ("standardizedPartitionedCellTargetsSubPath", self.config.standardized_partitioned_cell_to_targets_sub_folder),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)), ("geoStoreLastSuccessfulPrefix", "NA"),
             ("processedDataPath", self.config.processed_data_path)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def build_sensitive_places_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="build_sensitive_places_if_need",
            class_name="com.thetradedesk.jobs.delta.GetSensitivePlace",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("sensitivePlacesPrivacyRadiusInMeters", 50), ("sensitivePlacesPrivacyBrandPrefix", "ttd-blk-"),
             ("sensitivePlacesMinimumProcessedRatio", 0.01),
             ("builtSensitivePlacesPath", self.get_value(self.config.built_sensitive_places_path_key)),
             ("sensitivePlacesSourcePath", self.get_value(self.config.sensitive_places_source_path_key)),
             ("sensitivePlacesSourceOutputPath", self.get_value(self.config.sensitive_places_source_output_path_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    # replicated from build_sensitive_places_task for general polygon
    def build_sensitive_places_task_for_general_polygon(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="build_sensitive_places_if_need",
            class_name="com.thetradedesk.jobs.general.SensitivePlaceProcessor",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("sensitivePlacesPrivacyRadiusInMeters", 50), ("sensitivePlacesPrivacyBrandPrefix", "ttd-blk-"),
             ("sensitivePlacesMinimumProcessedRatio", 0.01),
             ("builtSensitivePlacesPath", self.get_value(self.config.built_sensitive_places_path_key)),
             ("sensitivePlacesSourcePath", self.get_value(self.config.sensitive_places_source_path_key)),
             ("sensitivePlacesSourceOutputPath", self.get_value(self.config.sensitive_places_source_output_path_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_geo_tiles_at_trunk_level_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="get_geo_tiles",
            class_name="com.thetradedesk.jobs.delta.GetGeoTilesAtTrunkLevel",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("fullBuildNumFiles", self.config.full_build_num_files),
             ("deltaBuildNumFiles", self.config.delta_build_num_files),
             ("geoStoreForcedTrunkLevelsPath", self.config.forced_trunk_levels_path),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("geoStoreLastSuccessfulPrefix", self.get_value(self.config.geo_store_last_successful_prefix_key)),
             ("sensitivePlacesSourcePath", self.get_value(self.config.sensitive_places_source_path_key)),
             ("sensitivePlacesSourceOutputPath", self.get_value(self.config.sensitive_places_source_output_path_key)),
             ('lastIncludedProcessedSensitivePlacesPath', self.get_value(self.config.last_included_sensitive_places_path_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def expand_geo_tiles_task(self, cluster, core_nums, file_index) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="expand_geo_tiles",
            class_name="com.thetradedesk.jobs.delta.ExpandGeoTiles",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("geoCacheEnv", "HDFS"), ("hdfsRootPath", self.config.HDFSRootPath),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("isFullBuild", self.config.is_full_build), ("trunkLevel", 10), ("maxLeafTargetCount", 500), ("partitionLevel", 5),
             ("levelBegin", 10), ("levelEnd", 16), ("partialMaxPerBin", 1000),
             ("geoStorePolygonCellMappingsPath", self.config.polygon_cell_mappings_path),
             ("fullBuildNumFiles", self.config.full_build_num_files), ("deltaBuildNumFiles", self.config.delta_build_num_files),
             ("geoStoreForcedTrunkLevelsPath", self.config.forced_trunk_levels_path), ("fileIndex", file_index)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def merge_diffCache_geotiles_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="merge",
            class_name="com.thetradedesk.jobs.delta.MergeDiffCacheAndGeoTiles",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("geoStoreLastSuccessfulPrefix", self.get_value(self.config.geo_store_last_successful_prefix_key)),
             ("isFullBuild", self.config.is_full_build)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def write_diffcache_data_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="convert_upload_diffcache_data",
            class_name="com.thetradedesk.jobs.delta.ConvertAndWriteDiffCache",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("geoStoreDiffCacheBucket", self.config.geo_store_diff_cache_bucket),
             ("geoStoreDiffCachePrefix", self.config.geo_store_diff_cache_prefix),
             ("geoStoreDiffCacheV3Prefix", self.config.geo_store_diff_cache_v3_prefix)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def convert_aerospike_data_task(self, cluster, core_nums, daily_convert='false') -> EmrJobTask:
        is_full_build: str = "true" if daily_convert == "true" else self.config.is_full_build.lower()
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="generate_aerospike_data_task",
            class_name="com.thetradedesk.jobs.delta.ConvertAerospikeData",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", is_full_build), ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_aerospike_push_cluster_and_task_in_parallel(self, cluster_label, aerospike_data_path):
        clusters = []
        for cluster, _ in GeoStoreUtils.delta_pipeline_aerospike_cluster_service_names.items():
            host = self.get_value(cluster)
            aws_region = GeoStoreUtils.aerospike_clusters_regions.get(cluster, "us-east-1")
            cluster_kwargs_by_region = GeoStoreUtils.cluster_kwargs_by_region[aws_region]

            cluster_task = self._create_cluster(
                f'{cluster_label}_{cluster}', self.aerospike_cluster_master_fleet_instance_type_configs,
                self.aerospike_cluster_core_fleet_instance_type_configs, aws_region, cluster_kwargs_by_region
            )

            job_task = EmrJobTask(
                name="push_to_aerospike",
                class_name="com.thetradedesk.jobs.delta.WriteGeoTilesToAerospike",
                executable_path=self.get_job_jar(),
                eldorado_config_option_pairs_list=[("geoStoreS3AerospikeDataPath", aerospike_data_path), ("aerospikeRetryMaxAttempts", 200),
                                                   ("aerospikeRetryInitialMs", 50), ("aerospikeTransactionRate", 1500),
                                                   ("aerospikeHosts", host),
                                                   ("aerospikeNamespace", GeoStoreUtils.delta_pipeline_aerospike_namespace),
                                                   ("aerospikeSet", GeoStoreUtils.delta_pipeline_aerospike_set),
                                                   ("useAerospikeTestCluster", "false"), ("aerospikeWriteEnabled", "true")],  # Test config
                additional_args_option_pairs_list=[
                    ("conf", "spark.executor.cores=5"),
                    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                ],
                region_name=aws_region
            )
            cluster_task.add_parallel_body_task(job_task)
            clusters.append(cluster_task)

        return clusters

    def get_expanding_cluster_and_task_in_parallel(self, cluster_label, partitions, core_nums):
        clusters = []
        for i in range(partitions):
            cluster_task = self._create_cluster(
                f'{cluster_label}_{i}', self.cluster_master_fleet_instance_type_configs,
                self.cluster_core_fleet_instance_type_configs(core_nums), "us-east-1", GeoStoreUtils.cluster_kwargs_by_region["us-east-1"]
            )
            cluster_task.add_sequential_body_task(self.expand_geo_tiles_task(cluster_task, core_nums, i))
            clusters.append(cluster_task)

        return clusters

    def get_copy_task(self, name, source_path, target_path) -> EmrJobTask:
        return EmrJobTask(
            name=name,
            class_name="",
            executable_path="aws",
            command_line_arguments=["s3", "cp", source_path, target_path, "--recursive"],
        )

    def get_stats_cluster(self, paths):
        cluster_task = self._create_cluster(
            'genStats', self.aerospike_cluster_master_fleet_instance_type_configs, self.aerospike_cluster_core_fleet_instance_type_configs,
            "us-east-1", GeoStoreUtils.cluster_kwargs_by_region["us-east-1"]
        )

        job_task = EmrJobTask(
            cluster_specs=cluster_task.cluster_specs,
            name="generate_delta_stats_task",
            class_name="com.thetradedesk.jobs.delta.GenStats",
            executable_path=self.get_job_jar(use_xcom_value=False),
            eldorado_config_option_pairs_list=self.java_settings_list + [("paths", paths)],
            additional_args_option_pairs_list=[("conf", "spark.executor.cores=5"),
                                               ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC")]
        )
        cluster_task.add_parallel_body_task(job_task)

        return cluster_task

    def get_stats_task(self, cluster, core_nums, key) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="generate_delta_stats_task",
            class_name="com.thetradedesk.jobs.delta.GenerateStats",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list + [("paths", self.get_value(key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_process_circles_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="standardize_circle_targets",
            class_name="com.thetradedesk.jobs.general.CircleTargetProcessor",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("fullBuildNumFiles", self.config.full_build_num_files),
             ("deltaBuildNumFiles", self.config.delta_build_num_files),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("geoStoreLastSuccessfulPrefix", self.get_value(self.config.geo_store_last_successful_prefix_key)),
             ("standardizedPartitionedCellTargetsSubPath", self.config.standardized_partitioned_cell_to_targets_sub_folder),
             ("sensitivePlacesSourcePath", self.get_value(self.config.sensitive_places_source_path_key)),
             ("sensitivePlacesSourceOutputPath", self.get_value(self.config.sensitive_places_source_output_path_key)),
             ('lastIncludedProcessedSensitivePlacesPath', self.get_value(self.config.last_included_sensitive_places_path_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_load_political_polygons_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="load_political_polygons",
            class_name="com.thetradedesk.jobs.general.PoliticalPolygonLoader",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("politicalPolygonSourcePath", self.config.political_polygon_source_path),
             ("polygonListTargetingDataOutputPath", self.config.political_polygon_list_targeting_data_output_path)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_process_political_polygons_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="process_political_polygons_into_standard",
            class_name="com.thetradedesk.jobs.general.PoliticalPolygonTargetProcessor",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("fullBuildNumFiles", self.config.full_build_num_files),
             ("deltaBuildNumFiles", self.config.delta_build_num_files),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("standardizedPartitionedCellTargetsSubPath", self.config.standardized_partitioned_cell_to_targets_sub_folder),
             ("polygonListTargetingDataOutputPath", self.config.political_polygon_list_targeting_data_output_path)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_assemble_cells_task(self, cluster, core_nums, file_index) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="assemble_cell_data",
            class_name="com.thetradedesk.jobs.general.CellDataAssembler",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("geoCacheEnv", "HDFS"), ("hdfsRootPath", self.config.HDFSRootPath),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("assembledCellDataSubPath", self.config.assembled_cell_data_sub_folder),
             ("standardizedPartitionedCellTargetsSubPath", self.config.standardized_partitioned_cell_to_targets_sub_folder),
             ("geoTileSizeBufferInByte", self.config.geotile_size_buffer_in_byte),
             ("geoTilePartialMatchLimit", self.config.geotile_partial_match_limit), ("isFullBuild", self.config.is_full_build),
             ("trunkLevel", 10), ("levelBegin", 10), ("levelEnd", 16), ("fileIndex", file_index)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_assembling_cluster_and_task_in_parallel(self, cluster_label, partitions, core_nums):
        clusters = []
        for i in range(partitions):
            cluster_task = self._create_cluster(
                f'{cluster_label}_{i}', self.cluster_master_fleet_instance_type_configs,
                self.cluster_core_fleet_instance_type_configs(core_nums), "us-east-1", GeoStoreUtils.cluster_kwargs_by_region["us-east-1"]
            )
            cluster_task.add_sequential_body_task(self.get_assemble_cells_task(cluster_task, core_nums, i))
            clusters.append(cluster_task)

        return clusters

    def get_shard_cell_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="shard_cell_task",
            class_name="com.thetradedesk.jobs.general.ShardCellProcessor",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("shardedCellDataSubfolder", self.config.sharded_cell_data_sub_folder),
             ("geoTileSizeBufferInByte", self.config.geotile_size_buffer_in_byte),
             ("geoTilePartialMatchLimit", self.config.geotile_partial_match_limit),
             ("assembledCellDataSubPath", self.config.assembled_cell_data_sub_folder),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    # replicated from convert_aerospike_data_task for general polygon pipeline
    def get_convert_aerospike_data_task(self, cluster, core_nums, daily_convert='false') -> EmrJobTask:
        is_full_build: str = "true" if daily_convert == "true" else self.config.is_full_build.lower()
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="generate_aerospike_data_task",
            class_name="com.thetradedesk.jobs.general.ConvertToAerospikeProcessor",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", is_full_build), ("shardedCellDataSubfolder", self.config.sharded_cell_data_sub_folder),
             ("geoTileSizeBufferInByte", self.config.geotile_size_buffer_in_byte),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_general_polygon_aerospike_push_cluster_and_task_in_parallel(self, cluster_label, aerospike_data_path):
        clusters = []
        for cluster, _ in GeoStoreUtils.general_polygon_pipeline_aerospike_cluster_service_names.items():
            host = self.get_value(cluster)
            aws_region = GeoStoreUtils.aerospike_clusters_regions.get(cluster, "us-east-1")
            cluster_kwargs_by_region = GeoStoreUtils.cluster_kwargs_by_region[aws_region]

            cluster_task = self._create_cluster(
                f'{cluster_label}_{cluster}', self.aerospike_cluster_master_fleet_instance_type_configs,
                self.aerospike_cluster_core_fleet_instance_type_configs, aws_region, cluster_kwargs_by_region
            )

            job_task = EmrJobTask(
                name="push_to_aerospike",
                class_name="com.thetradedesk.jobs.general.UploadToAerospikeProcessor",
                executable_path=self.get_job_jar(),
                eldorado_config_option_pairs_list=[
                    ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
                    ("geoStoreS3AerospikeDataPath", aerospike_data_path),
                    ("aerospikeRetryMaxAttempts", 200),
                    ("aerospikeRetryInitialMs", 50),
                    ("aerospikeTransactionRate", 1500),
                    ("aerospikeHosts", host),
                    ("isFullBuild", self.config.is_full_build),
                    ("aerospikeNamespace", GeoStoreUtils.general_polygon_pipeline_aerospike_namespace),
                    ("aerospikeSet", GeoStoreUtils.general_polygon__pipeline_aerospike_set),
                    # Test config
                    ("useAerospikeTestCluster", self.config.use_aerospike_test_cluster),
                    ("aerospikeWriteEnabled", "true"),
                    (
                        "aerospikeTestClusterUser",
                        "{{var.value.get('GEOSTORE_AEROSPIKE_TEST_CLUSTER_USER', 'targeting-testing-access-1702056778')}}"
                    ),
                    ("aerospikeTestClusterPassword", "{{var.value.get('GEOSTORE_AEROSPIKE_TEST_CLUSTER_PASSWORD', '')}}")
                ],
                additional_args_option_pairs_list=[
                    ("conf", "spark.executor.cores=5"),
                    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                ],
                region_name=aws_region
            )
            cluster_task.add_parallel_body_task(job_task)
            clusters.append(cluster_task)

        return clusters

    def get_convert_diff_cache_data_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="generate_diff_cache_data_task",
            class_name="com.thetradedesk.jobs.general.ConvertDiffCache",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build), ("shardedCellDataSubfolder", self.config.sharded_cell_data_sub_folder),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key))],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )

    def get_write_diff_cache_data_task(self, cluster, core_nums) -> EmrJobTask:
        return EmrJobTask(
            cluster_specs=cluster.cluster_specs,
            name="write_diff_cache_data_task",
            class_name="com.thetradedesk.jobs.general.WriteDiffCache",
            executable_path=self.get_job_jar(),
            eldorado_config_option_pairs_list=self.java_settings_list +
            [("isFullBuild", self.config.is_full_build),
             ("geoStoreCurrentPrefix", self.get_value(self.config.geo_store_current_prefix_key)),
             ("geoStoreDiffCacheV4Prefix", self.config.geo_store_diff_cache_v4_prefix),
             ("geoStoreDiffCacheBucket", self.config.geo_store_diff_cache_bucket)],
            additional_args_option_pairs_list=copy.deepcopy(self.get_additional_args(core_nums))
        )
