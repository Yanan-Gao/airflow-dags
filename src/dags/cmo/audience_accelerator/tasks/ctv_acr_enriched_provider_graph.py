from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.slack import slack_groups


class ACRTVIDHouseholdGraphGenerationJob:

    @staticmethod
    def get_task(config: AcrPipelineConfig) -> EmrClusterTask:
        java_options_list: List[Tuple[str, Any]] = [
            ("date", config.run_date), ("acrProvider", config.provider), ("acrCountry", config.country),
            (
                "acrGraphPath", SegmentDatasources(config.provider).get_acr_graph(config.country,
                                                                                  config.provider_graph_aggregate_version).get_root_path()
            ),
            (
                "aggOutPutPath", SegmentDatasources(config.provider
                                                    ).get_daily_graph_agg(config.country,
                                                                          config.provider_graph_aggregate_version).get_root_path()
            ), ("acrGraphVersion", config.provider_graph_aggregate_version)
        ]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'provider_graph_{config.provider}',
            master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
            core_fleet_instance_type_configs=
            getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, instance_capacity=3),
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations(),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            }
        )

        ####################################################################################################################
        # Steps
        ####################################################################################################################
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRTVIDHouseholdGraphGenerationJob',
                class_name="jobs.ctv.linear.acr.segment.tvidhouseholdgraph.ACRTVIDHouseholdGraphGenerationJob",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=2),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
