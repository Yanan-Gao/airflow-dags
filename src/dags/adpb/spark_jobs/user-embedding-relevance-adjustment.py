import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from datasources.sources.common_datasources import CommonDatasources
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

dag_name = 'adpb-user-embedding-relevance-adjustment'
owner = ADPB.team

# Job configuration
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
job_environment = TtdEnvFactory.get_from_system()
job_start_date = datetime(2024, 10, 20, 0, 0, 0)
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
num_workers = 180
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
qri_quantiles_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    depends_on_past=False,
    slack_tags=slack_tags,
    tags=[owner.jira_team],
    retries=0
)

bidfeedback_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='bidfeedback_data_available',
        datasets=[CommonDatasources.rtb_bidfeedback_v5],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

uer_adjustment_cluster = EmrClusterTask(
    name="user-embedding-qri-quantiles",
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

bidfeedback_qri_quantiles_sketch = EmrJobTask(
    name="BidFeedback_QriQuantilesJob",
    class_name="jobs.uer.QriQuantilesJob",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + eldorado_option_list + [("skewFactor", "50")],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=uer_adjustment_cluster.cluster_specs
)

adjustment_parameters_calculation = EmrJobTask(
    name="AdjustmentParametersJob",
    class_name="jobs.uer.AdjustmentParametersJob",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + eldorado_option_list,
    timeout_timedelta=timedelta(hours=1),
    cluster_specs=uer_adjustment_cluster.cluster_specs
)

uer_adjustment_cluster.add_sequential_body_task(bidfeedback_qri_quantiles_sketch)
uer_adjustment_cluster.add_sequential_body_task(adjustment_parameters_calculation)

bidfeedback_qri_quantiles_sketch >> adjustment_parameters_calculation

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=qri_quantiles_dag.airflow_dag))

qri_quantiles_dag >> bidfeedback_sensor >> uer_adjustment_cluster >> final_dag_check

airflow_dag = qri_quantiles_dag.airflow_dag
