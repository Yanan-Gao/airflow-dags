from airflow import DAG

from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.cluster_configs.disable_vmem_pmem_checks_configuration import DisableVmemPmemChecksConfiguration
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import FORECAST

standard_cluster_tags = {
    "Team": FORECAST.team.jira_team,
}

jar_path = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'

# Instance fleet configs
standard_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

standard_core_fleet_instance_type_configs = [
    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
]


def get_core_fleet_instance_type_configs(on_demand_capacity: int = 0):
    return EmrFleetInstanceTypes(
        instance_types=standard_core_fleet_instance_type_configs,
        on_demand_weighted_capacity=on_demand_capacity,
    )


tdu_xd_dag = TtdDag(
    dag_id="generate-tdu-x-device",
    start_date=datetime(2024, 10, 3, 4, 0),
    schedule_interval=timedelta(days=1),
    retries=2,
    retry_delay=timedelta(minutes=10),
    slack_channel=FORECAST.team.alarm_channel,
    slack_tags=FORECAST.team.jira_team,
    tags=[FORECAST.team.jira_team],
)

tdu_xd_cluster = EmrClusterTask(
    name="GenerateTduXDeviceCluster",
    additional_application_configurations=[DisableVmemPmemChecksConfiguration().to_dict()],
    master_fleet_instance_type_configs=standard_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(on_demand_capacity=1440),
    cluster_tags={
        **standard_cluster_tags, "Process": "GenerateTduXDevice"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

generate_tdu_xd_step = EmrJobTask(
    name="GenerateTduXDevice",
    class_name="com.thetradedesk.etlforecastjobs.universalforecasting.tdu.GenerateXDeviceTdu",
    timeout_timedelta=timedelta(hours=5, minutes=15),
    executable_path=jar_path,
    additional_args_option_pairs_list=[("executor-memory", "30000M"), ("conf", "spark.executor.cores=5"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=268"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=26000M"), ("conf", "spark.driver.cores=4"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=16000")],
    eldorado_config_option_pairs_list=[('date', '{{ ds }}'), ('lookbackDays', '3')]
)

tdu_xd_cluster.add_parallel_body_task(generate_tdu_xd_step)
tdu_xd_dag >> tdu_xd_cluster

dag: DAG = tdu_xd_dag.airflow_dag
