"""
Intermediate Cold Storage Digest Scan job

Job Details:
    - Expected to run in < 1 hour
    - Can only run one job at a time
"""

from datetime import timedelta, datetime

from airflow.exceptions import AirflowSensorTimeout
from airflow.models import TaskInstance
from airflow.sensors.python import PythonSensor

import dags.hpc.constants as constants
import dags.hpc.utils as hpc_utils
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

###########################################
# General Variables
###########################################
cadence_in_hours = 3

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4

# Prod Variables
dag_name = 'intermediate-cold-storage-key-scan'
schedule = f'0 */{cadence_in_hours} * * *'
el_dorado_jar_url = constants.HPC_AWS_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# el_dorado_jar_url = 's3://ttd-build-artefacts/eldorado/mergerequests/sba-HPC-4765-make-the-numbe-of-files-dynamic/latest/eldorado-hpc-assembly.jar'

###########################################
# DAG
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 4, 26),
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/I4ABG',
    run_only_latest=True,
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag


###########################################
# Check the SQS Queue before starting the spark job
###########################################
def skip_downstream_if_sensor_timeout_exception(context):
    task_instance: TaskInstance = context['task_instance']
    exception = context['exception']
    if isinstance(exception, AirflowSensorTimeout):
        task_instance.set_state("skipped")
        print("Marked the task as skipped because it failed due to sensor timeout.")


wait_till_sqs_queue_is_empty = OpTask(
    op=PythonSensor(
        task_id="wait_till_sqs_queue_is_empty",
        python_callable=hpc_utils.is_sqs_queue_empty,
        op_kwargs={
            'queue_name': constants.SQS_DATAIMPORT_INTERMEDIATEAGGREGATED_QUEUE,
            'region_name': constants.DEFAULT_AWS_REGION
        },
        poke_interval=timedelta(minutes=2),
        timeout=timedelta(hours=1, minutes=30),
        mode="reschedule",
        on_failure_callback=skip_downstream_if_sensor_timeout_exception
    )
)

###########################################
# Intermediate Cold Storage Scan
###########################################

intermediate_cold_storage_key_scan_class_name = 'com.thetradedesk.jobs.intermediatecoldstorage.IntermediateColdStorageKeyScan'
intermediate_cold_storage_key_scan_cluster_name = 'intermediate_cold_storage_key_scan_cluster'

intermediate_cold_storage_key_scan_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_xlarge().with_fleet_weighted_capacity(1),
                    M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1
)

intermediate_cold_storage_key_scan_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6g.r6g_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=17
)

intermediate_cold_storage_key_scan_additional_application_configurations = [{
    'Classification': 'yarn-site',
    'Properties': {
        'yarn.nodemanager.vmem-check-enabled': "false",
        'yarn.nodemanager.pmem-check-enabled': "false",
        'yarn.nodemanager.resource.cpu-vcores': "24"
    }
}]

intermediate_cold_storage_key_scan_cluster = EmrClusterTask(
    name=intermediate_cold_storage_key_scan_cluster_name,
    master_fleet_instance_type_configs=intermediate_cold_storage_key_scan_master_fleet_instance_type_configs,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    core_fleet_instance_type_configs=intermediate_cold_storage_key_scan_core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    additional_application_configurations=intermediate_cold_storage_key_scan_additional_application_configurations,
    enable_prometheus_monitoring=True,
    retries=0
)

intermediate_cold_storage_key_scan_props = [('conf', 'spark.network.timeout=3600s'), ('conf', 'spark.yarn.max.executor.failures=1000'),
                                            ('conf', 'spark.yarn.executor.failuresValidityInterval=1h'),
                                            ('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC'),
                                            ('conf', 'spark.yarn.maxAppAttempts=2')]

intermediate_cold_storage_key_scan_args = [
    ("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
    ('cadenceInHours', cadence_in_hours),
    ('address', constants.INTERMEDIATE_COLD_STORAGE_ADDRESS),
    ('namespace', 'ttd-scs'),
    ('recordsPerSecond', '150000'),
    ('digestsPerFileCount', '100000'),
]

intermediate_cold_storage_key_scan_step = EmrJobTask(
    name='intermediate_cold_storage_key_scan_step',
    class_name=intermediate_cold_storage_key_scan_class_name,
    cluster_specs=intermediate_cold_storage_key_scan_cluster.cluster_specs,
    additional_args_option_pairs_list=intermediate_cold_storage_key_scan_props,
    eldorado_config_option_pairs_list=intermediate_cold_storage_key_scan_args,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    executable_path=el_dorado_jar_url,
    timeout_timedelta=timedelta(hours=3),
    configure_cluster_automatically=True
)

intermediate_cold_storage_key_scan_cluster.add_parallel_body_task(intermediate_cold_storage_key_scan_step)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> wait_till_sqs_queue_is_empty >> intermediate_cold_storage_key_scan_cluster >> final_dag_check
