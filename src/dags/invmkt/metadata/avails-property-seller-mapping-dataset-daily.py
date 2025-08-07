from datetime import timedelta, datetime

from datasources.sources.invmkt_datasources import InvMktDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.cluster_params import Defaults
from ttd.ec2.emr_instance_types.general_purpose.m6a import M6a
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

# Prod variables
job_name = 'avails-property-seller-mapping-generation'
cluster_name = "invmkt_avails_property_seller_mapping"
invmkt = INVENTORY_MARKETPLACE().team
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5
job_schedule_interval = timedelta(days=1)
job_start_date = datetime(2025, 6, 1, 8)
job_environment = TtdEnvFactory.get_from_system()
max_active_runs = 1

# lw_import_task_id = 1000731, dbo.fn_enum_GatingType_ImportAvailsPropertySellerMappingMetadata()
lw_import_gating_type_id = 2000619
lw_import_log_type_id = 39

# Jar variables
jar_path = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-invmkt-assembly.jar'
class_name = 'jobs.metadata.AvailsPropertySellerDataMoverAggregation'

# Date and Partition configs
run_date = "{{ dag_run.logical_date.strftime(\"%Y-%m-%d\") }}"

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
    retry_delay=timedelta(minutes=5),
    tags=[invmkt.jira_team, 'avails-property-seller-mapping'],
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

master_instance_type = M6a.m6a_8xlarge()

instance_types = [
    M6a.m6a_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(3072)
    .with_ebs_iops(Defaults.MAX_GP3_IOPS).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
    M6a.m6a_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64).with_ebs_size_gb(6144)
    .with_ebs_iops(Defaults.MAX_GP3_IOPS).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
]

on_demand_weighted_capacity = 64 * 1

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[master_instance_type.with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity
)

avails_seller_property_mapping_cluster_task = EmrClusterTask(
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

####################################################################################################################
# steps
####################################################################################################################

config_list = [('date', run_date), ('datasetGenerationType', 'MetadataGeneration')]

job_step = EmrJobTask(
    name="spark_datasets_generation_step",
    class_name=class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=64g"),
        ("conf", "spark.sql.shuffle.partitions=2000"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=4),
    cluster_specs=avails_seller_property_mapping_cluster_task.cluster_specs
)

avails_seller_property_mapping_cluster_task.add_parallel_body_task(job_step)

wait_complete = DatasetCheckSensor(
    dag=dag.airflow_dag,
    ds_date="{{ logical_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    datasets=[InvMktDatasources.property_deal_metadata_daily],
    timeout=60 * 60 * 8
)

###########################################
#   MsSqlImportFromCloud
###########################################
open_lw_import_gate_task = MsSqlImportFromCloud(
    name=cluster_name,
    gating_type_id=lw_import_gating_type_id,
    log_type_id=lw_import_log_type_id,
    log_start_time=run_date,
    job_environment=job_environment,
    mssql_import_enabled=True
)

###########################################
#   Dependencies
###########################################
final_dag_check = FinalDagStatusCheckOperator(dag=adag)
dag >> avails_seller_property_mapping_cluster_task
wait_complete >> avails_seller_property_mapping_cluster_task.first_airflow_op()
job_step >> open_lw_import_gate_task
