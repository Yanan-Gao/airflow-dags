from datetime import timedelta, datetime

from datasources.sources.invmkt_datasources import InvMktDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datasources.sources.vertica_backups_datasource import VerticaBackupsDatasource
from ttd.tasks.op import OpTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

# Prod variables
job_name = 'mad-reporting-spend-aggregation'
cluster_name = "invmkt_marketplace_activity_dashboard"
invmkt = INVENTORY_MARKETPLACE().team
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5
job_schedule_interval = timedelta(hours=24)
job_start_date = datetime(2024, 7, 4, 7)
job_environment = TtdEnvFactory.get_from_system()
max_active_runs = 5
s3_bucket = 'ttd-marketplace-activity-dashboard'

# Jar variables
jar_path = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-invmkt-assembly.jar'
class_name = 'jobs.marketplaceactivitydashboard.MADSpendAggregation'
steps = [1, 2, 3]

# Date and Partition configs
run_date = "{{ dag_run.logical_date.strftime(\"%Y-%m-%d\") }}"
run_datetime = '{{ dag_run.logical_date.replace(hour=23, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S") }}'

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=max_active_runs,
    slack_channel=invmkt.alarm_channel,
    retries=1,
    retry_delay=timedelta(minutes=30),
    tags=[invmkt.jira_team, 'marketplace-activity-dashboard'],
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": invmkt.jira_team,
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        "start_date": job_start_date,
    }
)

adag = dag.airflow_dag

####################################################################################################################
# clusters
####################################################################################################################

base_ebs_size = 4000
instance_type = R5.r5_12xlarge()

instance_types = [
    instance_type.with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(1),
]
on_demand_weighted_capacity = 10
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[instance_type.with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity
)

mad_cluster_task = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": invmkt.jira_team},
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    emr_release_label=emr_release_label,
    environment=job_environment,
    cluster_auto_termination_idle_timeout_seconds=5 * 60,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }]
)

###########################################
# Dataset Check Sensor step
###########################################
check_rtb_platformreport_data_sensor_task = OpTask(
    op=DatasetCheckSensor(
        ds_date=run_datetime,
        task_id='check_rtb_platformreport_availability',
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        datasets=[VerticaBackupsDatasource.rtb_platformreport_vertica_backups_export],
        timeout=60 * 60 * 12  # wait up to 12 hours
    )
)

check_marketplace_metadata_dataset_data_sensor_task = OpTask(
    op=DatasetCheckSensor(
        ds_date=run_datetime,
        task_id='check_marketplace_metadata_dataset_availability',
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        datasets=[InvMktDatasources.marketplace_metadata_daily],
        timeout=60 * 60 * 12  # wait up to 12 hours
    )
)

####################################################################################################################
# steps
####################################################################################################################

for step in steps:
    config_list = [('date', run_date), ('step', step)]

    job_step = EmrJobTask(
        name="mad_step_" + str(step),
        class_name=class_name,  # no class name means its a .sh, will use jar path as script path
        executable_path=jar_path,
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=config_list,
        timeout_timedelta=timedelta(hours=3),
        cluster_specs=mad_cluster_task.cluster_specs
    )

    mad_cluster_task.add_sequential_body_task(job_step)

###########################################
#   Dependencies
###########################################
final_dag_check = FinalDagStatusCheckOperator(dag=adag)
dag >> check_rtb_platformreport_data_sensor_task >> check_marketplace_metadata_dataset_data_sensor_task >> mad_cluster_task
