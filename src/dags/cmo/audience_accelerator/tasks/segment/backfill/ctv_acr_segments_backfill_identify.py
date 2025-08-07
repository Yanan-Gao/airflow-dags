from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.datasources import Datasources
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.slack import slack_groups


class CtvAcrSegmentsBackfillIdentify:

    @staticmethod
    def get_task(config: AcrPipelineConfig) -> EmrClusterTask:

        job_name = 'back_fill' + "_" + config.provider

        java_options_list: List[Tuple[str, Any]] = [
            ("date", config.run_date), ("acrProviderName", config.provider), ("acrCountry", config.country), ("backfillDaysLookback", 7),
            (
                "outputPath", SegmentDatasources(config.provider).get_segment_dataset(config.country,
                                                                                      config.segment_enriched_version).get_root_path()
            ), ("acrProviderBrandSegmentsDirectory", Datasources.sql.acr_provider_segments.data_name)
        ]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=job_name,
            master_fleet_instance_type_configs=
            getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, ebs_size=100),
            cluster_tags={
                'Team': slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=
            getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=1),
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations(),
            enable_prometheus_monitoring=True,
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalSegmentBackfill',
                class_name="jobs.ctv.linear.acr.segment.tvidmapping.ACRUniversalSegmentBackfill",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=1),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
