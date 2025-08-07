from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.datperf.datasets import geronimo_dataset, pc_results_log_dataset, lost_bidrequest_dataset
from datetime import datetime, timedelta
from dags.datperf.utils.spark_config_utils import get_spark_args
from ttd.slack.slack_groups import DATPERF, AUDAUTO

java_settings_list = [("openlineage.enable", "false")]

PLUTUS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar"

plutus_result_logs_etl = TtdDag(
    dag_id="perf-automation-plutus-dataset-etl",
    start_date=datetime(2024, 8, 14, 0),
    schedule_interval=timedelta(hours=1),
    max_active_runs=4,
    enable_slack_alert=False,
    retries=0,
    run_only_latest=False,
    tags=["DATPERF", "Plutus"],
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutus_result_logs_etl.airflow_dag

protobuf_log_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='pc_log_data_available',
    poke_interval=60 * 20,
    timeout=60 * 60 * 24,
    # The logs for the hour 11:00 are generated between 11:05 and 12:20
    # So we wait for logs of the hour 1:00 to be generated to move forward (which should be around 1:05)
    ds_date='{{ data_interval_start.add(hours=2).to_datetime_string() }}',
    datasets=[pc_results_log_dataset.with_check_type("hour"),
              lost_bidrequest_dataset.with_check_type("hour")]
)

hourly_dataset_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='geronimo_available',
    poke_interval=60 * 30,
    timeout=60 * 60 * 12,
    ds_date="{{ data_interval_start.to_datetime_string() }}",
    datasets=[
        geronimo_dataset.with_check_type("hour"),
    ]
)

run_date = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:%M:%S\") }}"

# Instance configuration
instance_type = R5.r5_8xlarge()
on_demand_weighted_capacity = 10

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=5)
pc_spark_options_list = get_spark_args(cluster_params)

geronimo_pc_cluster = EmrClusterTask(
    name="GeronimoPCResultsETL",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            instance_type.with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_16xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=on_demand_weighted_capacity
    ),
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }],
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

pc_result_geronimo_join = EmrJobTask(
    name="PcResultsGeronimoJob",
    class_name="job.PcResultsGeronimoJob",
    additional_args_option_pairs_list=pc_spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [
        ('date', run_date),
    ],
    executable_path=PLUTUS_JAR
)

geronimo_pc_cluster.add_parallel_body_task(pc_result_geronimo_join)

plutus_result_logs_etl >> geronimo_pc_cluster

[protobuf_log_sensor, hourly_dataset_sensor] >> geronimo_pc_cluster.first_airflow_op()
