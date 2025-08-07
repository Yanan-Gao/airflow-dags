"""
HotCacheOptimization KPI Aggregation Job

Job Details:
    - Expected to run in under 10 minutes.
    - Can only run one job at a time.
    - One master node with reasonably high memory should suffice.
"""

# DAG airflow - these two words are needed as Airflow will do a basic validation to determine if a file contains a DAG definition or not.
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from dags.hpc import constants
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask

###########################################
# General Variables
###########################################
cadence_in_hours = 1
job_environment = TtdEnvFactory.get_from_system()
slack_channel = hpc.alarm_channel
cluster_tags = constants.DEFAULT_CLUSTER_TAGS
# Prod Variables
dag_name = 'hotcache-sib-missrate-aggregation'
schedule = f'0 */{cadence_in_hours} * * *'

###########################################
# DAG
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 11, 14),
    schedule_interval=schedule,
    slack_channel=slack_channel,
    run_only_latest=False,
    dag_tsg="https://thetradedesk.atlassian.net/wiki/x/dSMmAQ",
    tags=['HPC']
)
adag = dag.airflow_dag

###########################################
# Job
###########################################

job_class_name = 'com.thetradedesk.jobs.hotcachesibcount.HotCacheSibReportAggregation'
job_cluster_name = 'hpc_hcokpi_aggregation_cluster'
aggregation_step_name = 'aggregate-sib-missrate-reports'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

emr_cluster = EmrClusterTask(
    name=job_cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    environment=job_environment,
    enable_prometheus_monitoring=True
)

runtime_props = [
    ('conf', 'spark.network.timeout=300s'),
    ('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC'),
]

job_arguments = [
    ("processingDateHour", "{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
    ('cadenceInHours', 1),
]

aggregation_job_task = EmrJobTask(
    cluster_specs=emr_cluster.cluster_specs,
    name=aggregation_step_name,
    class_name=job_class_name,
    additional_args_option_pairs_list=runtime_props,
    eldorado_config_option_pairs_list=job_arguments,
    executable_path=constants.HPC_AWS_EL_DORADO_JAR_URL,
    timeout_timedelta=timedelta(hours=1),
    configure_cluster_automatically=True
)

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'

open_vertica_import_gate_task = OpTask(
    op=PythonOperator(
        dag=adag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': 2000340,  # ImportAdGroupHotCacheTruncationRateToVertica
                'grain': 100001,  # dbo.fn_Enum_TaskBatchGrain_Hourly()
                'dateTimeToOpen': '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}',
            },
        },
        task_id='hcokpi_open_vertica_import_gate_task'
    )
)

emr_cluster.add_sequential_body_task(aggregation_job_task)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> emr_cluster >> open_vertica_import_gate_task >> final_dag_check
