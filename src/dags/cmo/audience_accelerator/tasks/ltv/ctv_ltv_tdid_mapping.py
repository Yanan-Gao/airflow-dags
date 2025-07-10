from datetime import timedelta

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups


class LtvTdidMapping:

    @staticmethod
    def get_task(
        config: AcrPipelineConfig, job_type: str, output_file_count: int, core_fleet_type: EmrFleetInstanceTypes
    ) -> EmrClusterTask:
        job_name = job_type + '_tdid_mapping' + "_" + config.provider
        java_options_list = [("ttd.env", config.env.execution_env), ("date", config.run_date), ("provider", config.provider),
                             ("country", config.country), ("outputPathRoot", "s3://thetradedesk-useast-data-import/linear/segments-ttl"),
                             ("outputFileCount", output_file_count), ("graphMaxLookback", 14), ("jobType", job_type), ("version", 2)]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################
        cluster = EmrClusterTask(
            name=job_name,
            master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
            core_fleet_instance_type_configs=core_fleet_type,
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations(),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            }
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='LtvTdidMappingJob',
                class_name="jobs.ctv.linear.lighttv.LtvTdidMappingJob",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=2),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
