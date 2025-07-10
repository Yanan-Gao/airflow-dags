from datetime import timedelta

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.datasources import Datasources
from ttd.slack import slack_groups


class ACRSambaMonthlyAgg:

    @staticmethod
    def get_task(config: AcrPipelineConfig) -> EmrClusterTask:
        job_name = 'ctv-acr-samba-monthly-aggs'
        java_options_list = [("date", config.run_date), ("dailyAggPath", Datasources.samba.samba_daily_agg.get_root_path() + '/dailyAggs'),
                             ("brandOutputPath", Datasources.samba.samba_brand_agg.get_root_path() + '/brandAggs')]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'ctv-acr-samba-monthly-aggs_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
            cluster_tags={"Team": slack_groups.CMO.team.jira_team},
            core_fleet_instance_type_configs=
            getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, instance_capacity=4, ebs_size=256),
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                executable_path=config.jar,
                name=job_name + '_step',
                class_name="jobs.ctv.linear.acr.aggregates.ACRSambaMonthlyAggs",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=1),
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
