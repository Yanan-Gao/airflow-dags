import copy
from datetime import datetime, timedelta

from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = 'adpb-ae-data-aggregation'
owner = ADPB.team

# Job configuration
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
job_environment = TtdEnvFactory.get_from_system()
job_start_date = datetime(2024, 8, 20, 6, 0, 0)
job_schedule_interval = timedelta(hours=24)
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
slack_tags = owner.sub_team

cluster_tags = {
    "Team": owner.jira_team,
}
cluster_idle_timeout_seconds = 20 * 60

# Execution date
run_date = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
delta_days = 0

# Compute
worker_cores = 48
num_workers = 150
num_partitions = 2 * worker_cores * num_workers

master_instance_types = [M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)]
worker_instance_types = [
    M5.m5_12xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M5a.m5a_12xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M6g.m6g_12xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M5.m5_24xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
    M5a.m5a_24xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2)
]

# Application settings
java_settings_list = [
    ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"),
]

spark_options_list = [
    ("conf", "spark.driver.maxResultSize=32G"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % num_partitions),
]

eldorado_option_list = [("date", run_date)]

application_configuration = [{
    'Classification': 'spark',
    'Properties': {
        'maximizeResourceAllocation': 'true'
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "50",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

# DAG
ae_data_agg_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    depends_on_past=False,
    slack_tags=slack_tags,
    tags=[owner.jira_team],
    retries=0
)

check_bidrequest_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[RtbDatalakeDatasource.rtb_bidrequest_v5.with_check_type('day')],
        task_id="check_bidrequest_log",
        ds_date="{{ data_interval_start.strftime('%Y-%m-%d 00:00:00') }}",
        poke_interval=5 * 60,
        cloud_provider=CloudProviders.aws,
        timeout=60 * 60 * 6,  # 6 hours
    )
)

bidrequest_daily_aggregation_cluster = EmrClusterTask(
    name="audience-excluder-bidrequest-aggregation-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=worker_instance_types,
        on_demand_weighted_capacity=num_workers,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment
)

bidrequest_daily_aggregation = EmrJobTask(
    name="BidRequestUserAggregation",
    class_name="jobs.audienceexcluder.BidRequestUserAggregation",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + eldorado_option_list,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=bidrequest_daily_aggregation_cluster.cluster_specs
)

bidrequest_daily_aggregation_cluster.add_sequential_body_task(bidrequest_daily_aggregation)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=ae_data_agg_dag.airflow_dag))

ae_data_agg_dag >> check_bidrequest_sensor_task >> bidrequest_daily_aggregation_cluster >> final_dag_check

airflow_dag = ae_data_agg_dag.airflow_dag
