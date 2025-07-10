from datetime import timedelta, datetime

from dags.forecast.columnstore.columnstore_common_setup import (
    ColumnStoreEmrSetup,
    ColumnStoreDAGSetup,
)
from dags.forecast.columnstore.columnstore_sql import ColumnStoreFunctions
from dags.forecast.columnstore.enums.table_type import TableType
from dags.forecast.columnstore.enums.xd_level import XdLevel
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes

columnstore_sql_functions = ColumnStoreFunctions()
emr_steps_common_parameters = ColumnStoreEmrSetup()
columnstore_dag_common_parameters = ColumnStoreDAGSetup()

vector_value_class_name = "com.thetradedesk.etlforecastjobs.preprocessing.columnstore.provisioningexport.GenerateProvisioningExports"
vector_value_generation_step_name = "generate-vector-value-mappings"
cluster_name = "uf_columnstore_generate_vector_value_mappings"

run_date: str = "{{ data_interval_start.subtract(days=1).to_date_string() }}"

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
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
        ("date", run_date),
        ("enableLogging", "true"),
    ],
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.shuffle.partitions=100"),
    ],
    executable_path=emr_steps_common_parameters.job_jar,
    timeout_timedelta=timedelta(hours=1),
)

emr_cluster.add_parallel_body_task(generation_step)

loading_chain_of_tasks = columnstore_sql_functions.create_simple_load_task_group(
    loading_query="COPY ttd_forecast.VectorValueMappings",
    direct_copy=True,
    directory_root=f"s3://ttd-forecasting-useast/env=prod/columnstore/columnstoreExportDatasets/date={run_date}",
    task_id_base="load_vector_mappings_part",
    task_group_name="load_all_vector_mappings",
    on_load_complete=columnstore_sql_functions
    .mark_dataset_loaded(run_date, [xd_level for xd_level in XdLevel], TableType.VectorValueMappings)
)

dag_name = "uf-columnstore-vector-value-dag"
job_start_date = datetime(year=2024, month=4, day=1, hour=10, minute=0)
# Every day at 10 o'clock
job_schedule_interval = "0 10 * * *"
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
    default_args=columnstore_dag_common_parameters.default_args,
    dag_tsg=columnstore_dag_common_parameters.dag_tsg,
)

dag >> emr_cluster >> loading_chain_of_tasks

adag = dag.airflow_dag
