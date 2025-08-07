from datetime import timedelta, datetime

from dags.forecast.columnstore.columnstore_common_setup import ColumnStoreEmrSetup, ColumnStoreDAGSetup
from dags.forecast.columnstore.columnstore_sql import ColumnStoreFunctions
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes

columnstore_sql_functions = ColumnStoreFunctions()
emr_steps_common_parameters = ColumnStoreEmrSetup()
columnstore_dag_common_parameters = ColumnStoreDAGSetup()

vector_value_class_name = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.provisioningexport.GenerateDealCodeIntMappings"
vector_value_generation_step_name = "generate-deal-code-mappings"
cluster_name = "uf_columnstore_generate_deal-code_mappings"

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_4xlarge().with_ebs_size_gb(10).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_4xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

emr_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags=emr_steps_common_parameters.cluster_tags,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_steps_common_parameters.emr_release_label,
    log_uri=emr_steps_common_parameters.log_uri,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True,
)

generation_step = EmrJobTask(
    name=vector_value_generation_step_name,
    class_name=vector_value_class_name,
    eldorado_config_option_pairs_list=[
        ("date", "{{ ds }}"),
        ("enableLogging", "true"),
    ],
    executable_path=emr_steps_common_parameters.job_jar,
    timeout_timedelta=timedelta(hours=1),
)

emr_cluster.add_parallel_body_task(generation_step)

dag_name = "uf-columnstore-deal-code-mapping-dag"
job_start_date = datetime(year=2025, month=4, day=15, hour=6, minute=0)
# Every day at 6 o'clock
job_schedule_interval = "0 6 * * *"
active_running_jobs = 1

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=columnstore_dag_common_parameters.slack_channel,
    tags=columnstore_dag_common_parameters.tags,
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30),
    dag_tsg=columnstore_dag_common_parameters.dag_tsg,
    default_args=columnstore_dag_common_parameters.default_args,
)

dag >> emr_cluster

adag = dag.airflow_dag
