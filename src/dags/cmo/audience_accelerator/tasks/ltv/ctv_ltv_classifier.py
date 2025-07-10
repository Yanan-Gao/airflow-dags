from datetime import timedelta

from dags.cmo.utils.ltv_classifier_config import LtvClassifierConfig
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getMasterFleetInstances
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack import slack_groups
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes


class LtvClassifier:

    @staticmethod
    def get_task(config: LtvClassifierConfig, job_type: str, core_fleet_type: EmrFleetInstanceTypes) -> EmrClusterTask:
        job_name = 'ltv_classifier_%s_%s' % (job_type, config.country)
        java_options_list = [
            ("endDate", config.enrichment_date),  # endDate is exclusive
            ("timeWindowInDays", config.window),
            ("country", config.country),
            ("model_path", config.get_execution_model_path()),
            ("score_path", config.get_score_path()),
            ("debug", config.debug),
            ("ttd.ds.HouseholdFeatureLibraryDataSet.isInChain", "true")
        ]

        ####################################################################################################################
        # Cluster
        ####################################################################################################################
        cluster = EmrClusterTask(
            name=job_name,
            master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_type,
            enable_prometheus_monitoring=True,
            emr_release_label=config.emr_release_label,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        ####################################################################################################################
        # Steps
        ####################################################################################################################
        name = ''
        class_name = ''
        if job_type == 'fit':
            name = 'LtvHHClassifierJobFit'
            class_name = 'jobs.ctv.linear.lighttv.LtvHhClassifierFitting'
        else:
            raise Exception(f"Invalid job type {job_type}")

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name=name,
                class_name=class_name,
                eldorado_config_option_pairs_list=java_options_list,
                timeout_timedelta=timedelta(minutes=90),
                executable_path=config.jar,
                additional_args_option_pairs_list=config.get_step_additional_configurations()
            )
        )

        return cluster
