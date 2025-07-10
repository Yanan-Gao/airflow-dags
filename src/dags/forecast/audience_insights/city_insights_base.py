from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator

from dags.forecast.utils.team import TEAM_NAME
from datasources.sources.common_datasources import CommonDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask

execution_date_key = "execution_date"
jar_path = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"

pixel_limit = 45000

current_emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_3

standard_cluster_tags = {
    "Team": TEAM_NAME,
}

# Instance fleet configs
standard_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
)

standard_core_fleet_instance_type_configs = [
    R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
    R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
    R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48).with_ebs_size_gb(1536)
]


def get_core_fleet_instance_type_configs(on_demand_capacity: int = 0):
    return EmrFleetInstanceTypes(
        instance_types=standard_core_fleet_instance_type_configs,
        on_demand_weighted_capacity=on_demand_capacity,
    )


aerospike_hosts = "{{macros.ttd_extras.resolve_consul_url('ttd-lal.aerospike.service.useast.consul', port=3000, limit=1)}}"


class CityInsightsDag:

    def __init__(self, use_first_party: bool, total_uniques_count: int):
        self.use_first_party = use_first_party
        self.first_or_third_suffix = "1p" if self.use_first_party else "3p"
        self.total_uniques_count = total_uniques_count

        self.dag = self.create_dag()

    @staticmethod
    def get_avails_dependency(dag):
        return OpTask(
            op=CommonDatasources.avails_user_sampled.get_wait_for_day_complete_operator(
                dag,
                # we need to depend on the day before our execution date
                execution_delta=timedelta(days=1)
            )
        )

    def create_dag(self):
        use_first_party_config_value = str(self.use_first_party).lower()

        # The top level dag
        city_insights_dag = TtdDag(
            dag_id=f"audience-city-insights-{self.first_or_third_suffix}",
            start_date=datetime(2024, 9, 19, 0, 0),
            schedule_interval=timedelta(hours=6),
            retries=1,
            retry_delay=timedelta(minutes=10),
            tags=[TEAM_NAME],
            slack_tags=TEAM_NAME,
            enable_slack_alert=True,
            slack_channel="#dev-forecasting-alerts"
        )

        # clusters

        city_relevance_cluster = EmrClusterTask(
            name="CityRelevanceCluster",
            master_fleet_instance_type_configs=standard_master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(on_demand_capacity=960),
            cluster_tags={
                **standard_cluster_tags, "Process": f"CityRelevance-{self.first_or_third_suffix}"
            },
            enable_prometheus_monitoring=True,
            emr_release_label=current_emr_version
        )

        city_relevance_step = EmrJobTask(
            cluster_specs=city_relevance_cluster.cluster_specs,
            name="CityRelevance",
            class_name="com.thetradedesk.etlforecastjobs.audienceinsights.city.AudienceCityRelevance",
            executable_path=jar_path,
            eldorado_config_option_pairs_list=[("dateTime", "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"),
                                               ("useFirstParty", use_first_party_config_value),
                                               ("totalUniquesCount", self.total_uniques_count), ("totalPixelsCount", pixel_limit)],
            additional_args_option_pairs_list=[("executor-memory", "102000M"), ("conf", "spark.executor.cores=16"),
                                               ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=59"),
                                               ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                               ("conf", "spark.driver.memory=102000M"), ("conf", "spark.driver.cores=16"),
                                               ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                               ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                               ("conf", "spark.sql.shuffle.partitions=6000")],
            timeout_timedelta=timedelta(hours=6),
        )

        city_relevance_cluster.add_sequential_body_task(city_relevance_step)

        validate_insights_and_push_cluster = EmrClusterTask(
            name="ValidateAndPushCluster",
            master_fleet_instance_type_configs=standard_master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(on_demand_capacity=128),
            cluster_tags={
                **standard_cluster_tags, "Process": f"ValidateAndPushCityInsights-{self.first_or_third_suffix}"
            },
            enable_prometheus_monitoring=True,
            emr_release_label=current_emr_version
        )

        aerospike_spark_configs = [("executor-memory", "27000M"), ("conf", "spark.executor.cores=4"),
                                   ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=31"),
                                   ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                   ("conf", "spark.driver.memory=27000M"), ("conf", "spark.driver.cores=4"),
                                   ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                   ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                   ("conf", "spark.sql.shuffle.partitions=6000")]

        validate_insights_step = EmrJobTask(
            cluster_specs=validate_insights_and_push_cluster.cluster_specs,
            name="ValidateCityInsights",
            class_name="com.thetradedesk.etlforecastjobs.audienceinsights.city.ValidateCityInsights",
            executable_path=jar_path,
            eldorado_config_option_pairs_list=[("dateTime", "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"),
                                               ("useFirstParty", use_first_party_config_value)],
            additional_args_option_pairs_list=aerospike_spark_configs
        )

        push_to_aerospike_step = EmrJobTask(
            cluster_specs=validate_insights_and_push_cluster.cluster_specs,
            name="WriteCityInsightsToAerospike",
            class_name="com.thetradedesk.etlforecastjobs.audienceinsights.city.WriteCityInsightsToAerospike",
            executable_path=jar_path,
            eldorado_config_option_pairs_list=[("dateTime", "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"),
                                               ("useFirstParty", use_first_party_config_value), ("aerospikeSet", "ct"),
                                               ("aerospikeNamespace", "ttd-insights"), ("aerospikeHosts", aerospike_hosts)],
            additional_args_option_pairs_list=aerospike_spark_configs
        )

        validate_insights_and_push_cluster.add_sequential_body_task(validate_insights_step)
        validate_insights_and_push_cluster.add_sequential_body_task(push_to_aerospike_step)

        final_dummy = OpTask(op=DummyOperator(task_id="final_dummy", dag=city_insights_dag.airflow_dag))

        # don"t start any EMR jobs until we can be sure avails are ready
        avails_dependency = self.get_avails_dependency(city_insights_dag.airflow_dag)
        city_insights_dag >> avails_dependency >> city_relevance_cluster >> validate_insights_and_push_cluster >> final_dummy

        return city_insights_dag.airflow_dag
