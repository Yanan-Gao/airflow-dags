from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

from dags.cmo.audience_accelerator.tasks.ltv.ctv_ltv_classifier import LtvClassifier
from dags.cmo.audience_accelerator.tasks.ltv.ctv_ltv_tdid_mapping import LtvTdidMapping
from dags.cmo.audience_accelerator.tasks.ltv.ctv_ltv_user_embedding_loader import LtvUserEmbeddingLoader
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_segment_loader import ACRSegmentLoader
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances
from dags.cmo.utils.ltv_classifier_config import LtvClassifierConfig
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.eldorado.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
################################################################################################
# Configs -- Change things here for different providers
################################################################################################
from ttd.ttdenv import TtdEnvFactory

pipeline_name = "light-tv-viewers-us-v2"
job_start_date = datetime(2024, 8, 6, 8, 0)
job_schedule_interval = "0 11 * * 2"
latest_docker_image = "production.docker.adsrvr.org/ttd/ctv/python-script-runner:2944587"
data_delay = 15

env = TtdEnvFactory.get_from_system()

classifier_config = LtvClassifierConfig(
    provider=ACRProviders.Tivo,
    window=7,
    enrichment_date=f'{{{{(data_interval_end - macros.timedelta(days={data_delay - 1})).strftime(\"%Y-%m-%d\")}}}}',
    run_date="{{ (data_interval_end).strftime(\"%Y-%m-%d\") }}",
    country=Country.US,
    env=env,
    debug="true"
)

acr_config = AcrPipelineConfig(
    provider=ACRProviders.Tivo,
    country=Country.US,
    enriched_dataset=Datasources.tivo.generated.tivo_complete_enriched,
    data_delay=data_delay,
    run_date="{{ (data_interval_end).strftime(\"%Y-%m-%d\") }}",
    env=env,
    tdid_mapper_ds_version=2
)

################################################################################################
# DAG
################################################################################################
dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["ltv"],
    retries=0,
    run_only_latest=True
)

adag: DAG = dag.airflow_dag

################################################################################################
# Steps
################################################################################################
check_enrichment_data = OpTask(
    op=DatasetCheckSensor(
        datasets=[acr_config.enriched_dataset],
        ds_date=f'{{{{(data_interval_end - macros.timedelta(days={acr_config.data_delay})).to_datetime_string()}}}}',
        lookback=7,
        cloud_provider=CloudProviders.aws,
        task_id="check_enrichment_data",
        dag=adag,
        # poke every hour
        poke_interval=60 * 60,
        # wait up to 2 days
        timeout=60 * 60 * 48,
        # won't start if previous check failed
        depends_on_past=True
    )
)


def check_data(bucket: str, key: str):
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    if aws_storage.check_for_key(bucket_name=bucket, key=key):
        print(f's3://{bucket}/{key} exists')
        return True
    print(f's3://{bucket}/{key} does not exists')
    return False


ctv_feature_sensor = OpTask(
    op=PythonSensor(
        dag=adag,
        task_id="wait_for_feature_library",
        python_callable=check_data,
        op_kwargs={
            'bucket': 'ttd-ctv',
            'key': 'household-feature-library/US/prod/HouseholdFeatureLibraryV2/date={{ data_interval_end.strftime(\"%Y%m%d\") }}/_SUCCESS',
        },
        poke_interval=60 * 60 * 3,  # poke every 3 hours
        timeout=60 * 60 * 48,  # wait for 2 days
        mode='reschedule',
        # won't start if previous check failed
        depends_on_past=True
    )
)

ltv_classifier_fit_job = LtvClassifier.get_task(
    config=classifier_config,
    job_type='fit',
    core_fleet_type=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, instance_capacity=20)
)

save_model_path = OpTask(
    op=PythonOperator(
        task_id='save_model_path', python_callable=classifier_config.save_model_path, provide_context=True, do_xcom_push=True, dag=adag
    )
)

light_tv_fitting = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='linear-tv-data',
        image=latest_docker_image,
        name='ltv_fitting',
        task_id='ltv_fitting',
        dnspolicy='ClusterFirst',
        service_account_name='linear-tv-data',
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=500,
        log_events_on_failure=True,
        dag=adag,
        cmds=["python"],
        arguments=[
            "./executor.py", "--program=fit", f"--country={classifier_config.country}",
            f"--model_path=s3://ttd-ctv/light-tv/{classifier_config.country}/provider={classifier_config.provider}/fit/{classifier_config.env.dataset_write_env}/{classifier_config.get_saved_model_date()}",
            "--save_validation"
        ],
        env_vars=dict(PROMETHEUS_ENV=TtdEnvFactory.get_from_system().execution_env),
        resources=PodResources(request_cpu='4', request_memory='8G', limit_memory='16G', limit_ephemeral_storage='1G')
    )
)

light_tv_scoring = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='linear-tv-data',
        image=latest_docker_image,
        name='ltv_scoring',
        task_id='ltv_scoring',
        dnspolicy='ClusterFirst',
        service_account_name='linear-tv-data',
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=500,
        log_events_on_failure=True,
        dag=adag,
        cmds=["python"],
        arguments=[
            "./executor.py", "--program=score", f"--country={classifier_config.country}",
            f"--model_path=s3://ttd-ctv/light-tv/{classifier_config.country}/provider={classifier_config.provider}/fit/{classifier_config.env.dataset_write_env}/{classifier_config.get_saved_model_date()}",
            f"--score_path=s3://ttd-ctv/light-tv/{classifier_config.country}/provider={classifier_config.provider}/score/{classifier_config.env.dataset_write_env}/{{{{ data_interval_end.strftime(\"%Y%m%d\") }}}}",
            f"--feature_path=s3://ttd-ctv/household-feature-library/{classifier_config.country}/{classifier_config.env.dataset_write_env}/HouseholdFeatureLibraryV2/date={{{{ data_interval_end.strftime(\"%Y%m%d\") }}}}"
        ],
        env_vars=dict(PROMETHEUS_ENV=TtdEnvFactory.get_from_system().execution_env),
        resources=PodResources(request_cpu='4', request_memory='8G', limit_memory='16G', limit_ephemeral_storage='4G')
    )
)

ltv_tdid_mapping_job = LtvTdidMapping.get_task(
    config=acr_config,
    job_type=ACRSegmentLoader.LTV,
    output_file_count=100,
    core_fleet_type=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=8)
)

ltv_segment_loader = ACRSegmentLoader.get_task(
    config=acr_config,
    job_type=ACRSegmentLoader.LTV,
    output_file_count=12000,
    core_fleet_type=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=10),
    ttlInMinutes=10 * 24 * 60  # set the TTL to 10 days
)

ltv_user_embedding_loader = LtvUserEmbeddingLoader.get_task(
    config=acr_config,
    output_max_records_per_pile=500000,
    core_fleet_type=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=4),
)

run_scoring = OpTask(op=EmptyOperator(task_id="run_scoring", trigger_rule=TriggerRule.NONE_FAILED, dag=adag))

should_run_fitting = OpTask(
    op=BranchPythonOperator(
        task_id='job_fit_branch',
        python_callable=lambda logical_date, **_: ['check_enrichment_data', 'run_scoring']
        if logical_date.day in [11, 12, 13, 14, 15, 16, 17] else ['run_scoring'],
        provide_context=True,
        dag=adag
    )
)

################################################################################################
# Pipeline Structure
################################################################################################

dag >> ctv_feature_sensor >> should_run_fitting
should_run_fitting >> check_enrichment_data >> ltv_classifier_fit_job >> save_model_path >> light_tv_fitting >> run_scoring
should_run_fitting >> run_scoring >> light_tv_scoring

dag >> light_tv_scoring >> ltv_tdid_mapping_job >> ltv_segment_loader
dag >> light_tv_scoring >> ltv_user_embedding_loader
