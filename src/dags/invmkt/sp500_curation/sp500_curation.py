from datetime import timedelta, datetime

from dags.invmkt.sp500_curation.sp500_datasets import rtb_platform_metrics, sincera_site_metrics
from datasources.sources.invmkt_datasources import InvMktDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.cluster_params import Defaults
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from airflow.providers.cncf.kubernetes.secret import Secret

invmkt = INVENTORY_MARKETPLACE().team
job_schedule_interval_dev = timedelta(days=1)
job_schedule_interval_prod = timedelta(days=7)
job_start_date = datetime(2025, 5, 11, 1)
job_environment = TtdEnvFactory.get_from_system()
max_active_runs = 1
run_date = "{{ (dag_run.logical_date + macros.timedelta(days=4)).strftime(\"%Y-%m-%d\") }}"
dependency_date = "{{ (logical_date + macros.timedelta(days=4)).to_datetime_string() }}"

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5_2
job_name = 'sp500_curation_metrics'
cluster_name = "invmkt_sp500_curation"
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-invmkt-assembly.jar"
publisher_quality_metrics_class_name = "jobs.sp500curation.PublisherQualityMetricsGenerator"
property_quality_metrics_class_name = "jobs.sp500curation.PropertyQualityMetricsGenerator"

namespace_dev = "ttd-metadata-dev"
namespace_prod = "ttd-metadata-prod"
image_dev = "docker.pkgs.adsrvr.org/apps-prod/metadata-airflow:0.1.847"
image_prod = "docker.pkgs.adsrvr.org/apps-prod/metadata-airflow:0.1.847"
aspnetcore_environment_dev = "Development"
aspnetcore_environment_prod = "Production"
service_account = "metadata-airflow"


def get_env_value(env: str, dev_value: str, prod_value: str) -> str:
    return prod_value if env == TtdEnvFactory.prod else dev_value


# note: the application is in dev phase and is not enabled in prod.
dag: TtdDag = TtdDag(
    dag_id="sp500-curation",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval_prod if job_environment == TtdEnvFactory.prod else job_schedule_interval_dev,
    max_active_runs=max_active_runs,
    slack_channel=invmkt.alarm_channel,
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=[invmkt.jira_team, 'sp500-curation'],
    dagrun_timeout=timedelta(hours=6),
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": invmkt.jira_team,
    }
)

adag = dag.airflow_dag

####################################################################################################################
# dependencies
####################################################################################################################
property_seller_metadata_wait = DatasetCheckSensor(
    task_id="property_seller_metadata_check",
    dag=dag.airflow_dag,
    ds_date=dependency_date,
    poke_interval=60 * 20,  # poke every 20 minutes
    datasets=[InvMktDatasources.property_seller_metadata_daily],
    timeout=60 * 60 * 8  # wait up to 8 hours
)

rtb_platform_metrics_wait = DatasetCheckSensor(
    task_id="rtb_platform_metrics_check",
    dag=dag.airflow_dag,
    ds_date=dependency_date,
    poke_interval=60 * 20,  # poke every 20 minutes
    datasets=[rtb_platform_metrics],
    timeout=60 * 60 * 5  # wait up to 5 hours
)

sincera_site_metrics_wait = DatasetCheckSensor(
    task_id="sincera_site_metrics_check",
    dag=dag.airflow_dag,
    ds_date=dependency_date,
    poke_interval=60 * 20,  # poke every 20 minutes
    datasets=[sincera_site_metrics],
    timeout=60 * 60 * 5  # wait up to 5 hours
)

####################################################################################################################
# clusters
####################################################################################################################
master_instance_type = R6gd.r6gd_8xlarge()

instance_types = [
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(2048)
    .with_ebs_iops(Defaults.MAX_GP3_IOPS).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
]

on_demand_weighted_capacity = 128

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[master_instance_type.with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity
)

sp500_cluster_task = EmrClusterTask(
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
config_list = [('date', run_date)]

publisher_quality_metrics_job_step = EmrJobTask(
    name="publisher_quality_metrics_generation_step",
    class_name=publisher_quality_metrics_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=192g"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "maximizeResourceAllocation=true"),
        ("conf", "spark.sql.parquet.enableVectorizedReader=false"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=sp500_cluster_task.cluster_specs
)

sp500_cluster_task.add_sequential_body_task(publisher_quality_metrics_job_step)

property_quality_metrics_job_step = EmrJobTask(
    name="property_quality_metrics_generation_step",
    class_name=property_quality_metrics_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=192g"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "maximizeResourceAllocation=true"),
        ("conf", "spark.sql.parquet.enableVectorizedReader=false"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=sp500_cluster_task.cluster_specs
)

sp500_cluster_task.add_sequential_body_task(property_quality_metrics_job_step)

####################################################################################################################
# K8S pod
####################################################################################################################
batch_config_secret_volume = Secret(
    deploy_type='volume',
    deploy_target='/app/configOverrides/',
    secret='metadata-airflow-appsettings-secrets',
    key='appsettings.secrets.json'
)

sp500_property_curation_pod = TtdKubernetesPodOperator(
    namespace=get_env_value(job_environment, namespace_dev, namespace_prod),
    image=get_env_value(job_environment, image_dev, image_prod),
    name='SP500PropertyCurationPod',
    task_id="SP500PropertyCurationPod",
    dnspolicy='Default',
    get_logs=True,
    is_delete_operator_pod=True,
    dag=adag,
    env_vars={
        "ASPNETCORE_ENVIRONMENT": get_env_value(job_environment, aspnetcore_environment_dev, aspnetcore_environment_prod),
        "ASPNETCORE_URLS": "http://+;https://+",
    },
    startup_timeout_seconds=500,
    log_events_on_failure=True,
    service_account_name=service_account,
    annotations={
        'sumologic.com/include': 'true',
        'sumologic.com/sourceCategory': service_account,
        'prometheus.io/port': '80',
        'prometheus.io/scrape': 'true'
    },
    secrets=[batch_config_secret_volume],
    arguments=["PropertyCuration", run_date],
    resources=PodResources(limit_ephemeral_storage='500M', request_memory='4G', limit_memory='8G', request_cpu='1', limit_cpu='2')
)
sp500_property_curation_pod_task = OpTask(op=sp500_property_curation_pod)

sp500_publisher_curation_pod = TtdKubernetesPodOperator(
    namespace=get_env_value(job_environment, namespace_dev, namespace_prod),
    image=get_env_value(job_environment, image_dev, image_prod),
    name='SP500PublisherCurationPod',
    task_id="SP500PublisherCurationPod",
    dnspolicy='Default',
    get_logs=True,
    is_delete_operator_pod=True,
    dag=adag,
    env_vars={
        "ASPNETCORE_ENVIRONMENT": get_env_value(job_environment, aspnetcore_environment_dev, aspnetcore_environment_prod),
        "ASPNETCORE_URLS": "http://+;https://+",
    },
    startup_timeout_seconds=500,
    log_events_on_failure=True,
    service_account_name=service_account,
    annotations={
        'sumologic.com/include': 'true',
        'sumologic.com/sourceCategory': service_account,
        'prometheus.io/port': '80',
        'prometheus.io/scrape': 'true'
    },
    secrets=[batch_config_secret_volume],
    arguments=["PublisherCuration", run_date],
    resources=PodResources(limit_ephemeral_storage='500M', request_memory='4G', limit_memory='8G', request_cpu='1', limit_cpu='2')
)

sp500_publisher_curation_pod_task = OpTask(op=sp500_publisher_curation_pod)

###########################################
#   MsSqlImportFromCloud
###########################################
lw_import_gating_type_id = 2000706
lw_import_log_type_id = 152

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
# dag >> sp500_property_curation_pod_task
final_dag_check = FinalDagStatusCheckOperator(dag=adag)
dag >> sp500_cluster_task
sincera_site_metrics_wait >> sp500_cluster_task.first_airflow_op()
rtb_platform_metrics_wait >> sp500_cluster_task.first_airflow_op()
property_seller_metadata_wait >> sp500_cluster_task.first_airflow_op()
sp500_cluster_task >> sp500_property_curation_pod_task
sp500_property_curation_pod_task >> sp500_publisher_curation_pod_task
publisher_quality_metrics_job_step >> open_lw_import_gate_task
