from datetime import timedelta
from typing import List, Tuple, Any

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from datasources.sources.segment_datasources import SegmentDatasources
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups


class CtvAcrUniversalDailyFrequencyAggregation:

    def __init__(self, config: AcrPipelineConfig, back_fill: bool):
        self.job_suffix = 'freq_agg_' + config.provider + ("_back_fill" if back_fill else "")
        self.java_options_list: List[Tuple[str, Any]] = [(
            "s3ACRRootPath", SegmentDatasources(config.provider).get_segment_dataset(config.country,
                                                                                     config.segment_enriched_version).get_root_path()
        ), ("acrProvider", config.provider), ("country", config.country), ("date", config.run_date),
                                                         ("segmentEnrichedDsVersion", config.segment_enriched_version),
                                                         ("freqAggDsVersion", config.frequency_aggregation_version)]

        self.jar = config.jar
        self.config = config
        self.partitions = 400
        self.timeout_hours = 2
        self.data_delay = config.data_delay
        self.back_fill = back_fill

        if back_fill:
            self.java_options_list = self.java_options_list + [("useGeneration", "true"), ("daysToBackfill", 80),
                                                               ("generation", config.run_date), ("backfill", "true")]
            self.partitions = 20
            self.timeout_hours = 8
            self.data_delay = config.data_delay + 1

    def makeJobName(self, job_name):
        return job_name + "_" + self.job_suffix

    def get_daily_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("daily"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, ebs_size=256),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=6, ebs_size=500),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalDailyFrequencyAggregation',
                class_name="jobs.ctv.linear.acr.segment.frequencyaggregation.ACRUniversalDailyFrequencyAggregation",
                eldorado_config_option_pairs_list=self.java_options_list,
                timeout_timedelta=timedelta(hours=self.timeout_hours),
                executable_path=self.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_ten_day_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("ten_day"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, ebs_size=256),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=6, ebs_size=500),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalMultiDayFrequencyAggregation__10-Day',
                class_name="jobs.ctv.linear.acr.segment.frequencyaggregation.ACRUniversalMultiDayFrequencyAggregation",
                eldorado_config_option_pairs_list=self.java_options_list + [("daysToProcess", 10), ("partitions", self.partitions)],
                timeout_timedelta=timedelta(hours=self.timeout_hours),
                executable_path=self.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_ninety_day_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("ninety_day"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, ebs_size=256),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=5, ebs_size=500),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalMultiDayFrequencyAggregation__90-Day',
                class_name="jobs.ctv.linear.acr.segment.frequencyaggregation.ACRUniversalMultiDayFrequencyAggregation",
                eldorado_config_option_pairs_list=self.java_options_list + [("daysToProcess", 90), ("daysAggregated", 10),
                                                                            ("partitions", 800)],
                timeout_timedelta=timedelta(hours=self.timeout_hours),
                executable_path=self.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_daily_audience_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("daily_audience"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, ebs_size=256),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=10, ebs_size=500),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalDailyFrequencyAggregation',
                class_name="jobs.ctv.linear.acr.segment.frequencyaggregation.ACRUniversalMultiDayFrequencyAggregation",
                eldorado_config_option_pairs_list=self.java_options_list +
                [("daysToProcess", 1), ("inputSegmentType", "Audience"),
                 ("audienceDateDataPath", "s3://thetradedesk-useast-data-import/linear/acr/config/audiencesegmentdates"),
                 ("partitions", 800)],
                timeout_timedelta=timedelta(hours=self.timeout_hours),
                executable_path=self.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster

    def get_ninety_day_audience_aggregation(
        self, core_fleet_override: EmrFleetInstanceTypes = None, master_fleet_override: EmrFleetInstanceTypes = None
    ) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=self.makeJobName("ninety_day_audience"),
            master_fleet_instance_type_configs=master_fleet_override
            or getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, ebs_size=256),
            cluster_tags={
                "Team": slack_groups.CMO.team.jira_team,
            },
            core_fleet_instance_type_configs=core_fleet_override
            or getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=10, ebs_size=500),
            enable_prometheus_monitoring=True,
            emr_release_label=self.config.emr_release_label,
            additional_application_configurations=self.config.get_cluster_additional_configurations()
        )
        cluster.add_parallel_body_task(
            EmrJobTask(
                cluster_specs=cluster.cluster_specs,
                name='ACRUniversalMultiDayFrequencyAggregation__90-Day',
                class_name="jobs.ctv.linear.acr.segment.frequencyaggregation.ACRUniversalMultiDayFrequencyAggregation",
                eldorado_config_option_pairs_list=self.java_options_list +
                [("daysToProcess", 90), ("inputSegmentType", "Audience"),
                 ("audienceDateDataPath", "s3://thetradedesk-useast-data-import/linear/acr/config/audiencesegmentdates"),
                 ("partitions", 800)],
                timeout_timedelta=timedelta(hours=self.timeout_hours),
                executable_path=self.jar,
                additional_args_option_pairs_list=self.config.get_step_additional_configurations()
            )
        )

        return cluster
