from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.ttdenv import TtdEnvFactory

job_upstream_latency = 4  # hours

job_name = 'dooh-bid-feedback-filter'
job_start_date = datetime(2024, 8, 12, 12, 0)
job_schedule_interval = "0 * * * *"
job_environment = TtdEnvFactory.get_from_system()

job_hours_latency: timedelta = timedelta(hours=job_upstream_latency)

active_running_jobs = 4

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
gating_type_id = 2000126  # dbo.fn_enum_GatingType_ImportDoohCampaignVisualisation()
TaskBatchGrain_Hourly = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()
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
    retries=3,
    retry_delay=timedelta(minutes=10)
)

####################################################################################################################
# cluster
####################################################################################################################

base_ebs_size = 64

master_instance_type = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_instance_type = EmrFleetInstanceTypes(
    instance_types=[
        C7g.c7g_2xlarge().with_ebs_size_gb(base_ebs_size * 1).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        C7g.c7g_4xlarge().with_ebs_size_gb(base_ebs_size * 2).with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=2
)

dooh_bid_feedback_filter_cluster = EmrClusterTask(
    name="dooh_bid_feedback_filter_cluster",
    master_fleet_instance_type_configs=master_instance_type,
    cluster_tags={
        'Team': 'DATPRD',
    },
    core_fleet_instance_type_configs=core_instance_type,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%dT%H:00:00"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") - job_hours_latency
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

bid_feedback_filter_step = EmrJobTask(
    name="dooh_bid_feedback_filter_step",
    executable_path=jar_path,
    class_name="jobs.dooh.DoohCampaignVisualisationLookupJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('datetime', run_date_str_template),
                                       ('aggregationParquetRootPath', 's3a://ttd-identity/datapipeline/dooh'), ('partitions', '20')],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=dooh_bid_feedback_filter_cluster.cluster_specs
)

dooh_bid_feedback_filter_cluster.add_parallel_body_task(bid_feedback_filter_step)

# Trigger Vertica import via DataMover by opening the Gate
bid_feedback_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': gating_type_id,
                'grain': TaskBatchGrain_Hourly,
                'dateTimeToOpen': run_date_str_template
            }
        },
        task_id="bid_feedback_vertica_import_open_gate",
    )
)

dag >> get_job_run_datetime_task >> dooh_bid_feedback_filter_cluster >> bid_feedback_vertica_import_open_gate

adag = dag.airflow_dag
