from datetime import datetime, timedelta

from datasources.sources.avails_datasources import AvailsDatasources

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.kubernetes.secret import Secret

from ttd.ec2.emr_instance_types.storage_optimized.i3en import I3en
from ttd.el_dorado.v2.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import sav
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes

dag_id = "npi-active-rate-pipeline"
notification_slack_channel = "#scrum-sav-alerts"

npi_reporting_docker_image = "production.docker.adsrvr.org/ttd/npireporting:latest"
generator_task_trigger = 'Airflow'

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    profile = "staging"
    # no need to use k8s connection in prod test env as we've created the role binding for airflow2-service-account
    k8s_connection_id = None
    k8s_namespace = "npi-reporting-dev"
    service_account = "crm-data-service-account-staging"

    request_cpu = 1
    request_memory = 24
    limit_memory = 32
    request_ephemeral_storage = 16
    limit_ephemeral_storage = 64

    spark_env_path = "test"
else:
    profile = "prod"
    k8s_connection_id = 'npi-reporting-k8s'
    k8s_namespace = "npi-reporting"
    service_account = "crm-data-service-account-prod"

    request_cpu = 2
    request_memory = 48
    limit_memory = 80
    request_ephemeral_storage = 32
    limit_ephemeral_storage = 64

    spark_env_path = "prod"

k8s_resources = PodResources.from_dict(
    request_cpu=f"{request_cpu}",
    request_memory=f"{request_memory}Gi",
    limit_memory=f"{limit_memory}Gi",
    request_ephemeral_storage=f"{request_ephemeral_storage}Gi",
    limit_ephemeral_storage=f"{limit_ephemeral_storage}Gi"
)

java_opts = (f"-Xms{request_memory}g "
             "-XX:+UseG1GC "
             "-XX:MaxGCPauseMillis=200 ")

secrets = [
    Secret(
        deploy_type="env",
        deploy_target="TTD_SPRING_DATASOURCE_SNOWFLAKE_PASSWORD",
        secret="npi-datasource",
        key="%s_SNOWFLAKE_PASSWORD" % profile.upper()
    ),
    Secret(
        deploy_type="env",
        deploy_target="TTD_SPRING_DATASOURCE_PROVISIONING_PASSWORD",
        secret="npi-datasource",
        key="%s_PROVISIONING_PASSWORD" % profile.upper()
    ),
    Secret(
        deploy_type="env",
        deploy_target="TTD_SPRING_DATASOURCE_LOGWORKFLOW_PASSWORD",
        secret="npi-datasource",
        key="%s_LOGWORKFLOW_PASSWORD" % profile.upper()
    ),
    Secret(deploy_type="env", deploy_target="TTD_NPI_S3_SWOOP_ACCESSKEY", secret="npi-datasource", key="S3_SWOOP_ACCESSKEY"),
    Secret(deploy_type="env", deploy_target="TTD_NPI_S3_SWOOP_SECRETKEY", secret="npi-datasource", key="S3_SWOOP_SECRETKEY"),
]

# The job is scheduled to run daily at 5:00 AM to align with the availability of the "identity-agg-daily" data, which is typically ready around this time
job_schedule_interval = "0 5 * * *"
job_start_date = datetime(2024, 7, 1)

job_jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-sav-assembly.jar"
iqvia_id_export_path = f"s3a://ttd-sav-pii/env={spark_env_path}/npi_matchrate/idexport/"
daily_id_output_root = f"s3a://ttd-sav-pii/env={spark_env_path}/npi_matchrate/idexport/"
full_id_output_root = f"s3a://ttd-sav-pii/env={spark_env_path}/npi_matchrate/active/v1/"
file_date_path = "{{ logical_date.strftime(\"%Y%m%d\") }}"

ttd_dag = TtdDag(
    dag_id=dag_id,
    slack_channel=notification_slack_channel,
    enable_slack_alert=True,
    start_date=job_start_date,
    run_only_latest=True,
    schedule_interval=job_schedule_interval,
    retries=3,
    max_active_runs=1,
    depends_on_past=True,
    retry_delay=timedelta(minutes=1),
    tags=[sav.jira_team],
)
dag = ttd_dag.airflow_dag

id_export_generator_task = OpTask(
    op=TtdKubernetesPodOperator(
        kubernetes_conn_id=k8s_connection_id,
        namespace=k8s_namespace,
        service_account_name=service_account,
        image=npi_reporting_docker_image,
        image_pull_policy="Always",
        name="id_export_generator",
        task_id="id_export_generator",
        dnspolicy="ClusterFirst",
        get_logs=True,
        dag=dag,
        startup_timeout_seconds=600,
        execution_timeout=timedelta(hours=5),
        log_events_on_failure=True,
        resources=k8s_resources,
        secrets=secrets,
        env_vars={
            "SPRING_PROFILES_ACTIVE": profile,
            "TTD_NPI_APPLICATION_GENERATORTASKTYPE": "IdExportGenerator",
            "TTD_NPI_APPLICATION_GENERATORTASKTRIGGER": generator_task_trigger,
            "TTD_NPI_IDEXPORT_REPORTDATE": "{{ ds }}",
            "JAVA_OPTS": java_opts
        }
    )
)

active_id_generator_task = OpTask(
    op=TtdKubernetesPodOperator(
        kubernetes_conn_id=k8s_connection_id,
        namespace=k8s_namespace,
        service_account_name=service_account,
        image=npi_reporting_docker_image,
        image_pull_policy="Always",
        name="active_id_generator",
        task_id="active_id_generator",
        dnspolicy="ClusterFirst",
        get_logs=True,
        dag=dag,
        startup_timeout_seconds=600,
        execution_timeout=timedelta(hours=5),
        log_events_on_failure=True,
        resources=k8s_resources,
        secrets=secrets,
        env_vars={
            "SPRING_PROFILES_ACTIVE": profile,
            "TTD_NPI_APPLICATION_GENERATORTASKTYPE": "NpiActiveRateGenerator",
            "TTD_NPI_APPLICATION_GENERATORTASKTRIGGER": generator_task_trigger,
            "TTD_NPI_ACTIVERATE_REPORTDATE": "{{ ds }}",
            "JAVA_OPTS": java_opts
        }
    )
)

# Ref: https://thetradedesk.gitlab-pages.adsrvr.org/teams/aifun/TTD.AvailsPipeline.Contract/consuming/prod/#airflow-orchestration
wait_for_input_data = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id="wait-for-identity-agg-daily",
        ds_date="{{ logical_date.strftime(\"%Y-%m-%d %H:%M:%S\") }}",
        lookback=0,
        poke_interval=60 * 10,
        timeout=60 * 60 * 24,
        datasets=[AvailsDatasources.identity_agg_daily_dataset]
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3en.i3en_24xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3en.i3en_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=30
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

spark_options = [
    ("conf", "spark.driver.maxResultSize=0"),
    ("conf", "spark.network.timeout=2400s"),
    ("conf", "spark.executor.heartbeatInterval=2000s"),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
]

java_options = [
    ("AvailsDate", "{{ ds }}"),
    ("IqviaIdExportPath", f"{iqvia_id_export_path}/{file_date_path}_ttd_ids/"),
    ("LookBackDays", 7),
    ("DailyIdOutputPath", f"{daily_id_output_root}/"),
    ("FullIdOutputPath", f"{full_id_output_root}/date={file_date_path}/"),
]

cluster_task = EmrClusterTask(
    name="match-crosswalk-to-avails-ids",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": sav.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    additional_application_configurations=[additional_application_configurations],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
)

job_task = EmrJobTask(
    name="match-crosswalk-to-avails-ids-task",
    class_name="jobs.sav.matchrate.HcpActiveIdExporter",
    executable_path=job_jar,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=java_options,
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> id_export_generator_task >> wait_for_input_data >> cluster_task >> active_id_generator_task
