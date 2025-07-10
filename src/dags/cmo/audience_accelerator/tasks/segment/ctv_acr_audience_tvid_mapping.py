from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.datasources import Datasources
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups


class ACRAudienceTVIDMapping:

    @staticmethod
    def get_task(
        config: AcrPipelineConfig,
        days_to_process: int = 90,
        timeout: int = 1,
        core_fleet_override: EmrFleetInstanceTypes = None,
        master_fleet_override: EmrFleetInstanceTypes = None,
        provider_specific_java_options=None
    ) -> EmrClusterTask:
        if provider_specific_java_options is None:
            provider_specific_java_options = []
        java_options_list: List[Tuple[str, Any]] = [
            ("acrProviderName", config.provider), ("acrCountry", config.country),
            ("acrProviderSegmentsPath", Datasources.sql.acr_provider_segments.data_name),
            ("acrProviderDataBasePath", config.enriched_path_root), ("acrProviderAudiencePath", config.raw_data_path),
            (
                "outputRootPath", SegmentDatasources(config.provider).get_segment_dataset(config.country,
                                                                                          config.segment_enriched_version).get_root_path()
            ), ("daysToProcess", days_to_process), ("date", config.run_date)
        ] + provider_specific_java_options

        timeout_hours = timeout

        ###############################################################################################################
        # Cluster
        ###############################################################################################################

        cluster = EmrClusterTask(
            name=f'audience_tvid_mapping_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, ebs_size=100),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.SixteenX, instance_capacity=2, ebs_size=256),
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRAudienceSegmentTVIDMapper',
                class_name="jobs.ctv.linear.acr.segment.tvidmapping.ACRAudienceSegmentTVIDMapper",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=timeout_hours),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
