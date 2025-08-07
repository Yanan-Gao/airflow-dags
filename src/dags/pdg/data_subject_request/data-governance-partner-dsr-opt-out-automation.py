from datetime import datetime

from airflow.sensors.python import PythonSensor

from dags.pdg.data_subject_request.util import s3_utils
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.tasks.op import OpTask

###########################################
#   Job Configs
###########################################

job_name = 'data-governance-partner-dsr-opt-out-automation'
slack_channel = '#scrum-pdg-alerts'
job_start_date = datetime(2024, 7, 1)
job_schedule_interval = '7 6 * * *'  # Daily at 6:07 am UTC
job_type = "PartnerDsrOptOut"
pdg_jira_team = "PDG"

###########################################
#   DAG Setup
###########################################

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_channel,
    retries=0,
    tags=["Data Governance", pdg_jira_team, job_type],
)

# general configuration for jobs
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2
jar_path = "s3://ttd-build-artefacts/data-subject-requests/prod/jars/latest/data-subject-request-processing.jar"
job_class_name = "com.thetradedesk.dsr.optout.OptOutJobMain"
cluster_name = "dsr-optout"

# Bootstrap script for TLS to work on Aerospike on-prem. Imports TTD Root CA
# Using identity's team script since it is the same we need anyway
bootstrap_script_location = 's3://ttd-build-artefacts/data-subject-requests'
bootstrap_script = f'{bootstrap_script_location}/bootstrap.sh'

aws_region = 'us-east-1'

# reprocessReadyForOptOut will be respected over processS3File if both are provided
# Example trigger json
# {"processS3FileUri": "s3://ttd-data-subject-requests/sams/optout.csv"}
job_option_list = [
    ('dryRun', '{{ dag_run.conf.get("dryRun") if dag_run.conf is not none and dag_run.conf.get("dryRun") is not none else "false"}}'),
    (
        'reprocessReadyForOptOut',
        '{{ dag_run.conf.get("reprocessReadyForOptOut") if dag_run.conf is not none and dag_run.conf.get("reprocessReadyForOptOut") is not none else "false" }}'
    ),
    (
        'processS3FileUri',
        '{{ dag_run.conf.get("processS3FileUri") if dag_run.conf is not none and dag_run.conf.get("processS3FileUri") is not none }}'
    ),
]

# master/core instance types for all datasets
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

on_demand_weighted_capacity = 2

core_instance_types = [
    I3.i3_4xlarge().with_fleet_weighted_capacity(8),
]

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=core_instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity
)

scaling_policy = EmrClusterScalingProperties(
    maximum_capacity_units=on_demand_weighted_capacity * 100,
    minimum_capacity_units=on_demand_weighted_capacity,
    maximum_core_capacity_units=on_demand_weighted_capacity * 100,
    maximum_on_demand_capacity_units=on_demand_weighted_capacity
)

spark_options_list = [
    ("conf", "maximizeResourceAllocation=true"),
    # don't let step retry if step fails, fail the job directly for human debugging
    ("conf", "spark.yarn.maxAppAttempts=1")
]

adag = dag.airflow_dag

cluster_task = EmrClusterTask(
    name=f"{cluster_name}",
    emr_release_label=emr_release_label,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": "PDG",
        "Job": job_type
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    bootstrap_script_actions=[ScriptBootstrapAction(bootstrap_script, [bootstrap_script_location])],
    region_name=aws_region,
    managed_cluster_scaling_config=scaling_policy,
    cluster_auto_termination_idle_timeout_seconds=60 * 60,
    retries=1,
)

opt_out_step = EmrJobTask(
    name="optout",
    class_name=job_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=job_option_list,
    region_name=aws_region,
    retries=0,
)

monitor_opt_out_processing = OpTask(
    op=PythonSensor(
        task_id='monitor_successful_opt_out_processing',
        python_callable=s3_utils.validate_opt_out_success,
        timeout=60 * 60 * 12,  # Timeout in seconds (12 hours)
        poke_interval=60 * 60,  # 1 hour
        mode='poke',
        dag=adag,
    )
)

cluster_task.add_parallel_body_task(opt_out_step)

dag >> cluster_task >> monitor_opt_out_processing
