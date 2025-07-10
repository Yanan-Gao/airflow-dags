from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask

job_name = 'dooh-impression-multipliers-loader'
job_schedule_interval = "0 1 * * *"
job_start_date = datetime(2024, 8, 1, 1, 0)

avails_number_of_days_to_lookback = 0
im_number_of_days_to_lookback = 7

active_running_jobs = 1

jar_path: str = 's3://ttd-build-artefacts/channels/styx/snapshots/master/latest/styx-assembly.jar'

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel="#scrum-adpb-alerts",
    retries=1,
    retry_delay=timedelta(minutes=10)
)

####################################################################################################################
# clusters
####################################################################################################################

instance_types = [
    C7g.c7g_2xlarge().with_fleet_weighted_capacity(2),
    C7g.c7g_4xlarge().with_fleet_weighted_capacity(4),
    C7g.c7g_8xlarge().with_fleet_weighted_capacity(8),
    C7g.c7g_12xlarge().with_fleet_weighted_capacity(12)
]

ondemand_weighted_capacity = 48

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_8xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=ondemand_weighted_capacity
)

cluster = EmrClusterTask(
    name="dooh_impression_multiplier_interpolator_cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        'Team': 'DATPRD',
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%d"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S")
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


get_job_run_datetime_task = OpTask(
    op=PythonOperator(
        task_id='get_job_run_datetime',
        python_callable=get_job_run_datetime,
        op_kwargs=dict(execution_date='{{ ts_nodash }}'),
        provide_context=True,
        do_xcom_push=True
    )
)

run_date_str_template = f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{get_job_run_datetime_task.first_airflow_op().task_id}") }}}}'

####################################################################################################################
# steps
####################################################################################################################

default_config = [('conf', 'spark.dynamicAllocation.enabled=true'), ('conf', 'spark.speculation=false'),
                  ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'),
                  ('conf', 'spark.sql.files.ignoreCorruptFiles=true')]

on_target_config = list(default_config)
on_target_config.append(('conf', 'spark.driver.maxResultSize=37g'))

avails_step = EmrJobTask(
    name="agg_avails",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.arp.AggregateAvailsJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('date', run_date_str_template)],
    additional_args_option_pairs_list=on_target_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    cluster_specs=cluster.cluster_specs
)

im_step = EmrJobTask(
    name="impression_multiplier_loader",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.arp.ImpressionMultipliersLoadersJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('date', run_date_str_template), ('numberOfDaysToLookBack', im_number_of_days_to_lookback)],
    additional_args_option_pairs_list=on_target_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    cluster_specs=cluster.cluster_specs
)

avails_step >> im_step

cluster.add_parallel_body_task(avails_step)
cluster.add_parallel_body_task(im_step)

dag >> get_job_run_datetime_task >> cluster

adag = dag.airflow_dag
