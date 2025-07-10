from datetime import timedelta

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.slack import slack_groups


class CtvAcrSambaEnrichment:

    def __init__(self, config: AcrPipelineConfig, ignore_low_feed_match_rate: bool = False):
        self.config = config
        self.job_name = f"ctv_acr_samba_{config.country}_enrichment"
        self.cluster_name = self.job_name + '_cluster'
        self.job_class_name = "jobs.ctv.linear.acr.samba.SambaEnrichmentV3"
        self.master_clusters = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
        self.core_clusters = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=5)
        self.java_options_list = [("date", config.run_date),
                                  ("outputPath", Datasources.samba.samba_enriched_v3(config.country).get_root_path()),
                                  ("tvActivityPath", Datasources.segment("samba").get_tv_activity_dataset(config.country).get_root_path()),
                                  ("country", config.country), ("sambaFeedMatchRateThreshold", .65),
                                  ("ignoreLowMatchRate", "true" if ignore_low_feed_match_rate else "false")]
        self.job_timeout_delta = timedelta(hours=1)
        self.tags = {"Team": slack_groups.CMO.team.jira_team}

    def get_task(self, additional_spark_config_list=[]) -> EmrClusterTask:
        config = self.config

        ####################################################################################################################
        # Cluster
        ####################################################################################################################

        cluster = EmrClusterTask(
            name=self.cluster_name,
            master_fleet_instance_type_configs=self.master_clusters,
            cluster_tags=self.tags,
            core_fleet_instance_type_configs=self.core_clusters,
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_2,
            additional_application_configurations=config.get_cluster_additional_configurations()
        )

        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name=self.job_name + '_step',
                executable_path=config.jar,
                class_name=self.job_class_name,
                eldorado_config_option_pairs_list=self.java_options_list,
                timeout_timedelta=self.job_timeout_delta,
                additional_args_option_pairs_list=config.get_step_additional_configurations() + additional_spark_config_list
            )
        )

        # Return Created Sub-Dag
        return cluster
