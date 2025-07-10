from datetime import timedelta
from typing import List, Tuple

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups


class ACRSegmentLoader:
    WEEKLY = "Weekly"
    DAILY = "Daily"
    EXPOSURE = "Exposure"
    LTV = 'Ltv'

    @staticmethod
    def get_task(
        config: AcrPipelineConfig,
        job_type: str,
        output_file_count: int,
        core_fleet_type: EmrFleetInstanceTypes,
        additional_java_options: List[Tuple[str, str]] = [],
        ttlInMinutes: int = None,
        step_configuration: List[Tuple[str, str]] = [],
        sub_folder: str = None
    ) -> EmrClusterTask:
        sub_folder = sub_folder or f"{config.provider}-{config.country}-{job_type}"
        java_options_list = [("ttd.env", config.env.execution_env), ("date", config.run_date), ("acrProvider", config.provider),
                             ("acrCountry", config.country), ("outputPath", "hdfs:///linear/acr/collected/"),
                             ("finalOutputPath", "s3://thetradedesk-useast-logs-2/dataimport/collected/linear/acr/"),
                             ("outputFileNumber", output_file_count), ("jobType", job_type),
                             ("tdidMapperDsVersion", config.tdid_mapper_ds_version), ("subFolder", sub_folder)] + additional_java_options

        if ttlInMinutes is not None:
            java_options_list.append(("ttlInMinutes", ttlInMinutes))
        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=f'{job_type}_segment_loader_{config.provider}_{config.country}',
            master_fleet_instance_type_configs=
            getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, ebs_size=500),
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
                name='ACRSegmentLoadingJob',
                class_name="jobs.ctv.linear.acr.segment.segmentloading.ACRSegmentLoadingJob",
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(hours=2.5),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations() + step_configuration
            )
        )

        return cluster
