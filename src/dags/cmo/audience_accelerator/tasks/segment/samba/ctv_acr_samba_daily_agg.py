from datetime import timedelta

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.datasources import Datasources

from ttd.slack import slack_groups


class ACRSambaDailyAgg:

    @staticmethod
    def get_task(config: AcrPipelineConfig) -> EmrClusterTask:
        java_options_list = [("date", config.run_date), ("enrichedDataPath", Datasources.samba.samba_enriched.get_root_path()),
                             ("dailyAggPath", Datasources.samba.samba_daily_agg.get_root_path() + '/dailyAggs'),
                             ("country", config.country)]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################
        cluster = EmrClusterTask(
            name=f'ctv-acr-samba-daily-agg_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=
            getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, ebs_size=128),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=
            getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=4),
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                executable_path=config.jar,
                name='ACRSambaDailyAgg',
                class_name="jobs.ctv.linear.acr.aggregates.ACRSambaDailyAgg",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=1),
                additional_args_option_pairs_list=config.get_step_additional_configurations(),
            )
        )

        return cluster
