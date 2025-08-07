from airflow import DAG
from datetime import timedelta
from typing import List, Tuple

from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getMasterFleetInstances
from datasources.datasources import Datasources
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.cloud_provider import CloudProviders
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.identity_graphs.identity_graphs import IdentityGraphs
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.slack import slack_groups
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask


class ACRDailySegmentGeneration:

    @staticmethod
    def get_task(
        config: AcrPipelineConfig,
        core_fleet_type: EmrFleetInstanceTypes,
        dag: DAG,
        provider_specific_java_options: List[Tuple[str, str]] = [],
        step_configuration: List[Tuple[str, str]] = []
    ) -> ChainOfTasks:
        sub_folder = config.provider + "-" + config.country + '-daily'
        java_options_list = [
            ("date", config.run_date),
            (
                "acrProviderEnrichedPath",
                SegmentDatasources(config.provider).get_segment_dataset(config.country, config.segment_enriched_version).get_root_path()
            ), ("acrProvider", config.provider), ("acrCountry", config.country),
            (
                "acrGraphPath", SegmentDatasources(config.provider).get_acr_graph(config.country,
                                                                                  config.provider_graph_aggregate_version).get_root_path()
            ), ("subFolder", sub_folder), ("useCrosswalk", config.use_crosswalk),
            ("crosswalkGraphPath", Datasources.experian.experian_path), ("threshold", 0.8),
            ("segmentEnrichedDsVersion", config.segment_enriched_version), ("freqAggDsVersion", config.frequency_aggregation_version),
            ("acrGraphVersion", config.provider_graph_aggregate_version),
            ("acrProviderBrandSegmentsPath", Datasources.sql.acr_provider_segments.data_name)
        ] + provider_specific_java_options

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'daily_segment_generation_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=
            getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, ebs_size=50),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_type,
            use_on_demand_on_timeout=False,
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRDailySegmentGeneration',
                class_name="jobs.ctv.linear.acr.segment.tdidmapping.DailyCustomAndBrandSegmentGenerator",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=2),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations() + step_configuration
            )
        )

        check_graph_recency = OpTask(
            op=DatasetRecencyOperator(
                task_id="check_graph_daily",
                datasets_input=[IdentityGraphs().ttd_graph.v2.standard_input.households_capped_for_hot_cache.dataset],
                cloud_provider=CloudProviders.aws,
                lookback_days=14,
                dag=dag
            )
        )

        return ChainOfTasks(
            task_id="daily_segment_generation", tasks=[check_graph_recency, cluster]
        ).as_taskgroup("daily_segment_generation")
