from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "150",
        "fs.s3.sleepTimeSeconds": "10"
    }
}, {
    "Classification": "core-site",
    "Properties": {
        "fs.s3a.connection.maximum": "1000",
        "fs.s3a.threads.max": "50"
    }
}, {
    "Classification": "capacity-scheduler",
    "Properties": {
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}, {
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.vmem-check-enabled": "false"
    }
}, {
    "Classification": "spark-defaults",
    "Properties": {
        "spark.driver.maxResultSize": "40G",
        "spark.network.timeout": "360",
    }
}]

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

apv3_performance_metrics = TtdDag(
    dag_id="adpb-apv3-performance-metrics",
    start_date=datetime(2024, 7, 22, 12, 0, 0),
    schedule_interval=timedelta(days=1),
    max_active_runs=2,
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True
)
dag = apv3_performance_metrics.airflow_dag

cluster = EmrClusterTask(
    name="Apv3_Performance_Metrics",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(60).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_12xlarge().with_ebs_size_gb(180).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7g.r7g_16xlarge().with_ebs_size_gb(240).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=60
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

daily_aggregator_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="Performance_Daily_Aggregator",
    class_name="model.apv3.AdGroupDataElementPerformanceDailyDataProcessor",
    eldorado_config_option_pairs_list=[("runTime", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")],
    executable_path=jar_path
)

performance_metrics_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="Apv3_Performance_Metrics",
    class_name="model.apv3.PerformanceMetrics",
    eldorado_config_option_pairs_list=[("runTime", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"), ("conf", "spark.driver.memory=4G")],
    executable_path=jar_path
)

modelling_source_metrics_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="Generate_Modelling_Source_Info",
    class_name="model.apv3.GenerateModellingSourceInfo",
    eldorado_config_option_pairs_list=[("runTime", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")],
    executable_path=jar_path
)

job_datetime_format: str = "%Y-%m-%dT%H:00:00"
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
logworkflow_connection = 'lwdb'
gating_type_ids = [2000283, 2000284]


def _get_time_slot(dt: datetime):
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


def _open_lwdb_gate(**context):
    dt = _get_time_slot(context['data_interval_start'])
    log_start_time = dt.strftime('%Y-%m-%d %H:00:00')
    for gating_type_id in gating_type_ids:
        ExternalGateOpen(
            mssql_conn_id=logworkflow_connection,
            sproc_arguments={
                'gatingType': gating_type_id,
                'grain': TaskBatchGrain_Daily,
                'dateTimeToOpen': log_start_time
            }
        )


modelling_source_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        task_id='open_lwdb_gate',
        python_callable=_open_lwdb_gate,
        provide_context=True,
        dag=apv3_performance_metrics.airflow_dag,
    )
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=apv3_performance_metrics.airflow_dag))

cluster.add_sequential_body_task(daily_aggregator_step)
cluster.add_sequential_body_task(performance_metrics_step)
cluster.add_sequential_body_task(modelling_source_metrics_step)

apv3_performance_metrics >> cluster >> modelling_source_vertica_import_open_gate >> check
