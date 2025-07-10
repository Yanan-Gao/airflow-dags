from datetime import timedelta
from typing import List, Tuple, Any

from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, \
    getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.sources.segment_datasources import SegmentDatasources
from datasources.sources.sql_synced_datasources import SQLSyncedDataSources

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes

from ttd.slack import slack_groups


class CtvAcrSegmentInsights:

    aerospike_hosts = "{{ macros.ttd_extras.resolve_consul_url('aerospike-use-linear-tv-insights.aerospike.service.useast.consul', 3000, limit=1) }}"

    cluster_tags = {
        "Team": slack_groups.CMO.team.jira_team,
    }

    def __init__(self, config: AcrPipelineConfig):
        self.job_suffix = 'segment_insights' + "_" + config.provider
        self.java_options_list: List[Tuple[str, Any]] = [
            ("date", config.run_date), ("insightsTvidWeightsDate", config.run_date),
            (
                "s3ACRRootPath", SegmentDatasources(config.provider).get_segment_dataset(config.country,
                                                                                         config.segment_enriched_version).get_root_path()
            ), ("acrProvider", config.provider), ("country", config.country), ("segmentEnrichedDsVersion", config.segment_enriched_version),
            ("freqAggDsVersion", config.frequency_aggregation_version)
        ]

        self.config = config

    def makeJobName(self, job_name):
        return job_name + "_" + self.job_suffix

    def get_insights_weights_processor(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("weights_processor"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=6, ebs_size=500),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='InsightsWeightsProcessor',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.weights.InsightsWeightsProcessor",
                eldorado_config_option_pairs_list=self.java_options_list + [("partitions", 1)],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_insights_segment_processor(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("segment_processor"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=2, ebs_size=16),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='InsightsSegmentProcessor',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.InsightsSegmentProcessor",
                eldorado_config_option_pairs_list=self.java_options_list + [("daysToProcess", 1), ("partitions", 1)],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_frequency_segment_processor(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("frequency"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=2, ebs_size=16),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='FrequencySegmentProcessor',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.FrequencySegmentProcessor",
                eldorado_config_option_pairs_list=self.java_options_list + [("partitions", 20)],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_segment_aerospike_exporter(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("aerospike_exporter"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.EightX, instance_capacity=1, ebs_size=16),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='SegmentAerospikeExporter',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.aerospike.SegmentAerospikeExporter",
                eldorado_config_option_pairs_list=self.java_options_list + [("daysToProcess", 1), ("exportFrequency", "true"),
                                                                            ("exportInsights", "true"),
                                                                            ("aerospikeHosts", self.aerospike_hosts),
                                                                            ("aerospikeSet", "v2")],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_weights_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("weights_aggregation"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=2, ebs_size=16),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='InsightsWeightsAggregation',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.weights.InsightsWeightsAggregation",
                eldorado_config_option_pairs_list=self.java_options_list + [("partitions", 1)],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_universe_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("universe"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=3, ebs_size=16),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='UniverseSegmentProcessor',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.UniverseSegmentProcessor",
                eldorado_config_option_pairs_list=self.java_options_list +
                [("partitions", 1), ("s3DBSyncDataName", SQLSyncedDataSources.acr_provider_brand_segments.data_name)],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_aerospike_purge(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("aerospike_purge"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags=self.cluster_tags,
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=1, ebs_size=16),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='SegmentAerospikePurge',
                class_name="jobs.ctv.linear.acr.ltvsegmentinsights.aerospike.SegmentAerospikePurge",
                eldorado_config_option_pairs_list=self.java_options_list + [("daysToPurge", 4), ("aerospikeHosts", self.aerospike_hosts),
                                                                            ("aerospikeSet", "v2")],
                timeout_timedelta=timedelta(hours=2),
                executable_path=self.config.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster
