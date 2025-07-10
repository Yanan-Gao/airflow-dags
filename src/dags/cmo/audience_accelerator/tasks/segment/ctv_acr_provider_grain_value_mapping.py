from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.slack import slack_groups


class CtvAcrProviderGrainValueMapping:

    @staticmethod
    def get_task(config: AcrPipelineConfig, provider_specific_java_options: List[Tuple[str, str]] = []) -> EmrClusterTask:
        java_options_list: List[Tuple[str, Any]] = [
            ("date", config.run_date),
            ("outputPath", SegmentDatasources(config.provider).get_acr_provider_grain_value_dataset(config.country).get_root_path()),
            ("acrProviderName", config.provider),
            ("acrCountry", config.country),
            ("acrProviderEnrichedPath", config.enriched_path_root),
        ] + provider_specific_java_options

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'etl-provider-grain-value-mapping_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=
            getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.FourX, instance_capacity=5),
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRGrainValueMappingJob',
                class_name="jobs.ctv.linear.acr.segment.grainvaluemapping.ACRGrainValueMappingJob",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=2),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
