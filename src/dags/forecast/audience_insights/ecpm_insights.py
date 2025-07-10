# from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from dags.forecast.utils.team import TEAM_NAME
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask

jar_path = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"

current_emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_3

standard_cluster_tags = {
    "Team": TEAM_NAME,
}

# configs needed for el-dorado steps
execution_date_key = "execution_date"
aerospike_namespace = "ttd-insights"
aerospike_hosts = "{{macros.ttd_extras.resolve_consul_url('ttd-lal.aerospike.service.useast.consul', port=3000, limit=1)}}"
aerospike_set = "ecpm"
ttl = 21 * 86400  # 21 days before records expire unless updated
lookBackRangeInDays = 7

ecpm_insights_dag = TtdDag(
    dag_id="ecpm-insights",
    start_date=datetime(2024, 9, 19, 2, 0),
    schedule_interval=timedelta(days=1),
    retries=2,
    retry_delay=timedelta(hours=1),
    tags=[TEAM_NAME],
    slack_tags=TEAM_NAME,
    enable_slack_alert=True,
    slack_channel="#dev-forecasting-alerts",
)


def set_timestamp(**context):
    timestamp = context["templates_dict"]["ds"]
    print(f"Setting {execution_date_key} to {timestamp}")
    context["task_instance"].xcom_push(key=execution_date_key, value=timestamp)


set_timestamps_and_iso_weekday_task = OpTask(
    op=PythonOperator(
        task_id="set_timestamps_and_iso_weekday", python_callable=set_timestamp, provide_context=True, templates_dict={"ds": "{{ ds }}"}
    )
)

standard_master_fleet_instance_types = [R5.r5_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(1)]

standard_core_fleet_instance_types = [
    M5.m5_12xlarge().with_ebs_size_gb(1500).with_max_ondemand_price().with_fleet_weighted_capacity(3),
    M5.m5_24xlarge().with_ebs_size_gb(1500).with_max_ondemand_price().with_fleet_weighted_capacity(6)
]

data_generation_spark_configs = [("num-executors", "143"), ("executor-memory", "37G"), ("executor-cores", "5"),
                                 ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                 ("conf", "spark.yarn.maxAppAttempts=1"), ("conf", "spark.driver.maxResultSize=50G"),
                                 ("conf", "spark.sql.shuffle.partitions=12000"), ("conf", "spark.dynamicAllocation.enabled=true"),
                                 ("conf", "spark.driver.memory=100G")]

sites_ecpm_cluster = EmrClusterTask(
    name="SitesECPMCluster",
    master_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_master_fleet_instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_core_fleet_instance_types, on_demand_weighted_capacity=96),
    cluster_tags={
        **standard_cluster_tags, "Process": "SitesECPM"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

sites_ecpm_step = EmrJobTask(
    cluster_specs=sites_ecpm_cluster.cluster_specs,
    name="SitesECPM",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.ecpm.GenerateSitesECPM",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("lookBack", lookBackRangeInDays)],
    additional_args_option_pairs_list=data_generation_spark_configs,
    timeout_timedelta=timedelta(hours=4)
)

sites_ecpm_cluster.add_sequential_body_task(sites_ecpm_step)

geos_ecpm_cluster = EmrClusterTask(
    name="GeosECPMCluster",
    master_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_master_fleet_instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_core_fleet_instance_types, on_demand_weighted_capacity=96),
    cluster_tags={
        **standard_cluster_tags, "Process": "GeosECPM"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

geos_ecpm_step = EmrJobTask(
    cluster_specs=geos_ecpm_cluster.cluster_specs,
    name="GeosECPM",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.ecpm.GenerateGeosECPM",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("lookBack", lookBackRangeInDays)],
    additional_args_option_pairs_list=data_generation_spark_configs,
    timeout_timedelta=timedelta(hours=8)
)

geos_ecpm_cluster.add_sequential_body_task(geos_ecpm_step)

third_party_ecpm_cluster = EmrClusterTask(
    name="ThirdPartyECPMCluster",
    master_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_master_fleet_instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_core_fleet_instance_types, on_demand_weighted_capacity=96),
    cluster_tags={
        **standard_cluster_tags, "Process": "ThirdPartyECPM"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

third_party_ecpm_step = EmrJobTask(
    cluster_specs=third_party_ecpm_cluster.cluster_specs,
    name="ThirdPartyECPM",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.ecpm.GenerateThirdPartySegmentsECPM",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}")],
    additional_args_option_pairs_list=data_generation_spark_configs,
    timeout_timedelta=timedelta(hours=8)
)

third_party_ecpm_cluster.add_sequential_body_task(third_party_ecpm_step)

# generate aerospike records cluster definition
# validate cluster definition
# write to aerospike cluster definition

aerospike_ecpm_cluster = EmrClusterTask(
    name="ValidateAndWriteToAerospikeCluster",
    master_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_master_fleet_instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=standard_core_fleet_instance_types, on_demand_weighted_capacity=4),
    cluster_tags={
        **standard_cluster_tags, "Process": "ProcessECPMRecordsIntoAerospike"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

aerospike_spark_configs = [("num-executors", "5"), ("executor-memory", "37G"), ("executor-cores", "5"),
                           ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.yarn.maxAppAttempts=1"),
                           ("conf", "spark.driver.maxResultSize=50G"), ("conf", "spark.sql.shuffle.partitions=12000"),
                           ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.driver.memory=100G")]

generate_aerospike_records_step = EmrJobTask(
    cluster_specs=aerospike_ecpm_cluster.cluster_specs,
    name="GenerateAerospikeRecords",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.ecpm.GenerateECPMAerospikeRecords",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("lookBack", lookBackRangeInDays), ("ttl", ttl)],
    additional_args_option_pairs_list=aerospike_spark_configs,
    timeout_timedelta=timedelta(hours=1)
)

validate_aerospike_records_step = EmrJobTask(
    cluster_specs=aerospike_ecpm_cluster.cluster_specs,
    name="ValidateECPMAerospikeRecordsStep",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.ecpm.ValidateECPMData",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}")],
    additional_args_option_pairs_list=aerospike_spark_configs,
    timeout_timedelta=timedelta(hours=1)
)

write_records_to_aerospike_step = EmrJobTask(
    cluster_specs=aerospike_ecpm_cluster.cluster_specs,
    name="WriteECPMRecordsToAerospike",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.ecpm.WriteECPMDataToAerospike",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("aerospikeSet", aerospike_set), ("aerospikeNamespace", aerospike_namespace),
                                       ("aerospikeHosts", aerospike_hosts)],
    additional_args_option_pairs_list=aerospike_spark_configs,
    timeout_timedelta=timedelta(hours=1)
)

aerospike_ecpm_cluster.add_sequential_body_task(generate_aerospike_records_step)
aerospike_ecpm_cluster.add_sequential_body_task(validate_aerospike_records_step)
aerospike_ecpm_cluster.add_sequential_body_task(write_records_to_aerospike_step)

# dummy step
final_dummy = OpTask(op=DummyOperator(task_id="final_dummy", dag=ecpm_insights_dag.airflow_dag))

ecpm_insights_dag >> set_timestamps_and_iso_weekday_task

# Need to find the date to use and SIB partition to pass into spark jobs.
# Generate all the ecpm data before attempting to validate and export.
set_timestamps_and_iso_weekday_task >> sites_ecpm_cluster >> aerospike_ecpm_cluster
set_timestamps_and_iso_weekday_task >> geos_ecpm_cluster >> aerospike_ecpm_cluster
set_timestamps_and_iso_weekday_task >> third_party_ecpm_cluster >> aerospike_ecpm_cluster

# Ensures errors are propagated correctly.
aerospike_ecpm_cluster >> final_dummy

dag = ecpm_insights_dag.airflow_dag
