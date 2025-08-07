from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.slack import slack_groups


class CtvAcrEnrichedProviderDailyGraphAggregation:

    @staticmethod
    def get_task(config: AcrPipelineConfig) -> EmrClusterTask:
        java_options_list: List[Tuple[str, Any]] = [
            ("date", config.run_date), ("acrProviderEnrichedPath", config.enriched_path_root),
            (
                "aggOutPutPath", SegmentDatasources(config.provider).get_segment_dataset(config.country,
                                                                                         config.segment_enriched_version).get_root_path()
            ), ("acrProvider", config.provider), ("acrCountry", config.country), ("outputVersion", config.provider_graph_aggregate_version)
        ]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'daily_graph_agg_{config.provider}',
            master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=
            getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=2),
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
                name='ACRDailyProviderGraphAggregate',
                class_name="jobs.ctv.linear.acr.segment.tvidhouseholdgraph.ACRDailyProviderGraphAggregate",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=2),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
