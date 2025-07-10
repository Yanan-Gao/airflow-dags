import logging
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.pipeline_config import PipelineConfig
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

job_name = 'ctv-acr-daily-analysis-v2'
active_running_jobs = 1
job_schedule_interval = timedelta(hours=4)
job_start_date = datetime(2024, 10, 1, 16, 0) - job_schedule_interval
job_days_lookback = 30
job_hours_timeout = 3
pipeline_config = PipelineConfig()

###########################################
#   Passed to Job
###########################################

ttd_env = TtdEnvFactory.get_from_system()

# Inputs
inputDataBucket = "thetradedesk-useast-data-import"
brandsPath = "/acr/daily-analysis/selectedPrometheusAnalysisBrands.csv"
networksPath = "/acr/daily-analysis/selectedPrometheusAnalysisNetworks.csv"

# Outputs
outputDataBucket = "thetradedesk-useast-data-import"
outputSuccessPrefix = f"linear/acr/success/{ttd_env.dataset_write_env}/analysis"
sparkJobOutputPath = "s3a://thetradedesk-useast-data-import/linear/acr"


class ACRProviderConfig:

    def __init__(
        self,
        name,
        success_enrichment_data_path_date_template,
        enriched_data_path,
        emr_instances,
        date_format,
        countries=["US"],
        lookback_days=job_days_lookback,
        audiences_path="",
        base_name=""
    ):
        self.name = name
        self.success_enrichment_data_path_date_template = success_enrichment_data_path_date_template
        self.enriched_data_path = enriched_data_path
        self.emr_instances = emr_instances
        self.date_format = date_format
        self.countries = countries
        self.lookback_days = lookback_days
        self.audiences_path = audiences_path
        self.base_name = name if base_name == "" else base_name
        # audiences_path will be empty for non audience jobs
        self.emr_job_name = f"ACR{self.base_name.capitalize()}{audiences_path.capitalize()}DailyAnalysis"


acr_provider_configs = [
    ACRProviderConfig(
        'samba', f'linear/acr/{ttd_env.dataset_read_env}/samba-enriched/country={{1}}/v=2/date={{0}}/_SUCCESS', 'linear/acr', 4, "%Y%m%d",
        ["AU"]
    ),
    ACRProviderConfig(
        'samba-CA',
        f'linear/acr/{ttd_env.dataset_read_env}/samba-enriched/country={{1}}/v=2/date={{0}}/_SUCCESS',
        'linear/acr',
        4,
        "%Y%m%d", ["CA"],
        base_name="samba"
    ),
    ACRProviderConfig(
        'tivo',
        f'linear/acr/{ttd_env.dataset_read_env}/tivo-enriched/v=2/date={{0}}/_SUCCESS',
        'linear/acr',
        10,
        "%Y%m%d",
        lookback_days=10
    ),
    ACRProviderConfig(
        'fwm', f'linear/acr/{ttd_env.dataset_read_env}/fwm-enriched/v=3/date={{0}}/_SUCCESS', 'linear/acr', 18, "%Y%m%d", lookback_days=20
    ),
    ACRProviderConfig(
        'fwm-audiences',
        f'linear/acr/{ttd_env.dataset_read_env}/acrSegmentFrequency/provider=fwm-audiences/country=US/days=90/v=5/date={{0}}/generation={{0}}/_SUCCESS',
        'fwm',
        10,
        "%Y%m%d",
        lookback_days=3,
        audiences_path='audience',
        base_name="fwm"
    ),
]


def find_dates_to_run(**kwargs):
    acr_provider = kwargs['acr_provider']
    hook = AwsCloudStorage(conn_id='aws_default')

    date = datetime.strptime(kwargs['templates_dict']['start_date'], "%Y-%m-%d") - timedelta(days=acr_provider.lookback_days - 1)

    for x in range(0, acr_provider.lookback_days):
        date_check_string = date.strftime("%Y-%m-%d")
        success_file_path = f"{outputSuccessPrefix}/{acr_provider.name}/{date_check_string}"
        logging.info(f'Checking {date}. Bucket: {outputDataBucket} Prefix: {success_file_path}')

        # First day found which hasn't yet run breaks the search loop and triggers job
        if not hook.check_for_key(bucket_name=outputDataBucket, key=success_file_path):
            logging.info(f'{date_check_string} has not yet run!')
            break

        if x == (acr_provider.lookback_days - 1):
            logging.info('All days ran!')
            return 'prepare_for_updating_tables'

        date = date + timedelta(days=1)

    for country in acr_provider.countries:

        process_date = date.strftime(acr_provider.date_format)
        input_path = acr_provider.success_enrichment_data_path_date_template.format(process_date, country)

        logging.info(
            f'Checking {acr_provider.name.capitalize()} data from {process_date}. '
            f'Bucket: {inputDataBucket} Path: {input_path}'
        )

        if not hook.check_for_key(bucket_name=inputDataBucket, key=input_path):
            logging.info(f'{acr_provider.name.capitalize()} data does not exist!')
            return 'prepare_for_updating_tables'

    kwargs['task_instance'].xcom_push(key='run_date', value=date.strftime("%Y-%m-%d"))

    logging.info(f'Data exists! Running {process_date}')
    return f'select_subnets_spark-job-{acr_provider.name}'


def write_success_file(**kwargs):
    acr_provider = kwargs['acr_provider']
    run_date = kwargs['task_instance'].xcom_pull(dag_id=job_name, task_ids=f'find_dates_to_run_{acr_provider.name}', key='run_date')

    hook = AwsCloudStorage(conn_id='aws_default')

    success_key = f"{outputSuccessPrefix}/{acr_provider.name}/{run_date}"
    hook.load_string(string_data='', key=success_key, bucket_name=outputDataBucket, replace=True)

    return f'Written key {success_key}'


cluster_tags = {
    'process': job_name,
    'Team': CMO.team.jira_team,
}

sparkConfig = {
    'spark.speculation': 'false',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.executor.extraJavaOptions': '-server -XX:+UseParallelGC',
    'spark.sql.files.ignoreCorruptFiles': 'true'
}


def build_acr_provider_cluster(acr_provider_config: ACRProviderConfig, pip_config: PipelineConfig):
    run_date = f"{{{{ task_instance.xcom_pull(dag_id='{job_name}', " \
               f"task_ids='find_dates_to_run_{acr_provider_config.name}', key='run_date') }}}}"

    master_fleet_instance_type_configs = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX)
    core_fleet_instance_type_configs = getFleetInstances(
        EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.FourX, instance_capacity=acr_provider_config.emr_instances, on_demand=True
    )

    cluster_task = EmrClusterTask(
        name=f"spark-job-{acr_provider_config.name}",
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,  # The job breaks on spark 3.5, so had to pin it here
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        cluster_tags=cluster_tags,
        environment=ttd_env,
        additional_application_configurations=pip_config.get_cluster_additional_configurations()
    )

    driver_java_options = {
        "date": run_date,
        "enrichedDataPath": f"s3a://{inputDataBucket}/{acr_provider_config.enriched_data_path}",
        "storageLocation": sparkJobOutputPath,
        "brandsPath": brandsPath,
        "networksPath": networksPath,
        "country": ",".join(acr_provider_config.countries),
        "ACRProvider": acr_provider_config.name
    }

    eldorado_config_option_pairs_list = list(driver_java_options.items())

    job_task = EmrJobTask(
        name=acr_provider_config.emr_job_name,
        class_name=f"jobs.ctv.linear.acr.daily_analysis.{acr_provider_config.emr_job_name}",
        executable_path=pip_config.jar,
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
        additional_args_option_pairs_list=[("conf", f"spark.sql.shuffle.partitions={40 * acr_provider_config.emr_instances}")] +
        pip_config.get_step_additional_configurations(),
    )
    cluster_task.add_parallel_body_task(job_task)

    return cluster_task


test_mode = False

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    max_active_runs=1,  # (optional) For jobs with expensive resources, best to limit concurrency
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    run_only_latest=True,
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        "start_date": job_start_date,
    }
)

adag = dag.airflow_dag

prepare_for_updating_tables_op = OpTask(
    op=EmptyOperator(
        task_id='prepare_for_updating_tables', start_date=job_start_date, retries=3, trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )
)

for acr_provider in acr_provider_configs:
    find_dates_to_run_op = OpTask(
        op=BranchPythonOperator(
            task_id=f'find_dates_to_run_{acr_provider.name}',
            templates_dict={'start_date': '{{ yesterday_ds }}'},
            op_kwargs={'acr_provider': acr_provider},
            provide_context=True,
            python_callable=find_dates_to_run
        )
    )

    write_success_file_op = OpTask(
        op=PythonOperator(
            task_id=f'write_success_file_{acr_provider.name}',
            op_kwargs={'acr_provider': acr_provider},
            provide_context=True,
            python_callable=write_success_file
        )
    )

    acr_provider_cluster = build_acr_provider_cluster(acr_provider, pipeline_config)

    dag >> find_dates_to_run_op >> acr_provider_cluster >> write_success_file_op >> prepare_for_updating_tables_op

for group in ['brands', 'shows', 'networks', 'dmas', 'brands_with_dmas', 'creatives']:
    prepare_for_updating_tables_op >> OpTask(
        op=AWSAthenaOperator(
            task_id=f'update_athena_partitioned_table_{group}',
            output_location=f's3://{inputDataBucket}/linear/acr/athena-output',
            query=f'MSCK REPAIR TABLE {group};',
            retries=3,
            sleep_time=60,
            aws_conn_id='aws_default',
            database='acr_analysis'
        )
    )
