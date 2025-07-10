from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.datasources import Datasources
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups


class ACRSegmentTVIDMapping:

    @staticmethod
    def get_task(
        config: AcrPipelineConfig,
        back_fill: bool,
        timeout: int = 1,
        core_fleet_override: EmrFleetInstanceTypes = None,
        master_fleet_override: EmrFleetInstanceTypes = None,
        provider_specific_java_options: List[Tuple[str, str]] = []
    ) -> EmrClusterTask:

        job_name = 'segment_tvid_mapping' + "_" + config.provider
        java_options_list: List[Tuple[str, Any]] = [
            ("date", config.run_date), ("acrProviderEnrichedPath", config.enriched_path_root),
            ("acrProviderBrandSegmentsDirectory", Datasources.sql.acr_provider_segments.data_name),
            (
                "outputPath", SegmentDatasources(config.provider).get_segment_dataset(config.country,
                                                                                      config.segment_enriched_version).get_root_path()
            ), ("acrProviderName", config.provider), ("acrCountry", config.country),
            ("segmentEnrichedDsVersion", config.segment_enriched_version)
        ] + provider_specific_java_options

        instances = 5
        timeout_hours = timeout

        if back_fill:
            job_name += "_back_fill"
            instances = 15
            timeout_hours = 20
            java_options_list += {("backfill", "true"), ("backfillDaysLookback", 1), ("daysToProcesses", 80),
                                  ("generation", config.run_date), ("outputFileCount", 100)}

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'{job_name}_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, ebs_size=100),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=instances, ebs_size=128),
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        ####################################################################################################################
        # Steps
        ####################################################################################################################
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalSegmentTVIDMapperJob',
                class_name="jobs.ctv.linear.acr.segment.tvidmapping.ACRUniversalSegmentTVIDMapper",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=timeout_hours),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
