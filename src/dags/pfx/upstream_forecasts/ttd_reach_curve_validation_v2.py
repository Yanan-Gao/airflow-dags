import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.operators.python import PythonOperator
from dags.tv.constants import FORECAST_JAR_PATH
from dags.pfx.upstream_forecasts.ctv_cpm_adjustments import xcom_template_to_get_value
from dags.pfx.upstream_forecasts.maxreach_setup_tasks import get_max_reach_cluster, get_maxreach_task
from dags.pfx.upstream_forecasts.validation_pipeline_tasks import get_validation_pipeline_cluster, \
    get_validation_pipeline_task
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

###########################################
#   job settings
###########################################
validation_class_namespace = "com.thetradedesk.etlforecastjobs.upstreamforecasting.validation"
validation_dag_id = "ctv-upstream-forecast-validation-v2"
store_dates_task_id = "store_conf_dates_to_xcom"
jar_path = FORECAST_JAR_PATH
ttd_env = "prod"
max_look_back_for_base_line_date = 100  # assume we run validation quaterly at least, so 100 days should enough to locate a previous date

# Run date is specified in conf in order to allow airflow runs for the same run date
reach_curve_target_date_key = "reach-curve-target-date"
demo_target_date_key = "demo-target-date"
validation_base_date_key = "validation-base-date"

validation_pipeline_dag_pipeline: TtdDag = TtdDag(
    dag_id=validation_dag_id,
    start_date=datetime(2024, 7, 1, 0, 0),
    schedule_interval=None,
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "validation", "reach curve"],
    max_active_runs=1
)

dag = validation_pipeline_dag_pipeline.airflow_dag

validation_cluster = get_validation_pipeline_cluster("reach_curve_validation_cluster")
maxreach_cluster = get_max_reach_cluster("reach_curve_maxreach_cluster")
gating_cluster = get_validation_pipeline_cluster("reach_curve_validation_gating_cluster")

functional_validation = get_validation_pipeline_task(
    cluster=validation_cluster,
    class_name=f"{validation_class_namespace}.functionalvalidation.FunctionalValidationJob",
    job_name="functional_validation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)

wikipedia_comparison = get_validation_pipeline_task(
    cluster=validation_cluster,
    class_name=f"{validation_class_namespace}.comparisonjobs.WikipediaComparisonJob",
    job_name="wikipedia_comparison_generation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=4,
    env=ttd_env,
    jar_path=jar_path
)

world_bank_comparison = get_validation_pipeline_task(
    cluster=validation_cluster,
    class_name=f"{validation_class_namespace}.comparisonjobs.WorldBankComparisonJob",
    job_name="world_bank_comparison_generation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=4,
    env=ttd_env,
    jar_path=jar_path
)

max_reach_setup = get_maxreach_task(
    cluster=maxreach_cluster,
    demo_date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, demo_target_date_key),
    reach_curve_date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=7,
    env=ttd_env,
    jar_path=jar_path
)

adbrain_comparison = get_validation_pipeline_task(
    cluster=validation_cluster,
    class_name=f"{validation_class_namespace}.comparisonjobs.AdBrainComparisonJob",
    job_name="adbrain_comparison_generation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)

census_vs_cross_device_comparison_interpretation = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.interpretationjobs.CensusVsCrossDeviceComparisonInterpretationJob",
    job_name="census_vs_cross_device_comparison_interpretation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)

wikipedia_comparison_interpretation = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.interpretationjobs.WikipediaComparisonInterpretationJob",
    job_name="wikipedia_comparison_interpretation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)

world_bank_comparison_interpretation = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.interpretationjobs.WorldBankComparisonInterpretationJob",
    job_name="world_bank_comparison_interpretation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)

adbrain_comparison_interpretation = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.interpretationjobs.AdBrainComparisonInterpretationJob",
    job_name="adbrain_comparison_interpretation",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)

export_master_validated_forecasts = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.ExportValidatedForecastsJob",
    job_name="export_master_validated_forecasts",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)


def get_closest_previous_date(target_date: str) -> Any | None:
    most_recent_date = Datasources.ctv.upstream_forecast_validation_pass_rate.check_recent_data_exist(
        cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
        ds_date=datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=1),
        max_lookback=max_look_back_for_base_line_date
    )

    if most_recent_date.is_success:
        base_date_str = most_recent_date.get().strftime("%Y-%m-%d")
        logging.info(f'Found most recent date {base_date_str}')
        return base_date_str
    logging.info(f'Did not find any previous date for target date {target_date}')
    return None


analyze_validated_results = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.ValidatedResultsAnalyzingJob",
    job_name="analyze_validated_results",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    base_date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, validation_base_date_key),
    need_base_date=True,
    timeout=1,  # 1 hour, small job, times out quicker
    env=ttd_env,
    jar_path=jar_path
)

format_validation_results = get_validation_pipeline_task(
    cluster=gating_cluster,
    class_name=f"{validation_class_namespace}.ExportFormattingJob",
    job_name="format_validation_results",
    date=xcom_template_to_get_value(validation_dag_id, store_dates_task_id, reach_curve_target_date_key),
    timeout=2,
    env=ttd_env,
    jar_path=jar_path
)


def store_dates(**context):
    # store keys for downstream fmap gen dag to use
    task_instance = context["task_instance"]
    demo = context["dag_run"].conf[demo_target_date_key]
    curve = context["dag_run"].conf[reach_curve_target_date_key]
    task_instance.xcom_push(key=demo_target_date_key, value=demo)
    task_instance.xcom_push(key=reach_curve_target_date_key, value=curve)
    base_date = get_closest_previous_date(curve)
    if base_date is not None:
        print(f"xcom put -> derived validation-base-date:{base_date}")
        task_instance.xcom_push(key=validation_base_date_key, value=base_date)
    else:
        # pass in date with year 0, this is a workaround for not storing it at all
        # which is not working for call get_validation_pipeline_step_sub_dag;
        # using subbag with template string to get xcom value is not really working for null value from xcom
        # it passes in the scala function as a str "None" string and thus it will fail the date parser on scala job
        task_instance.xcom_push(key=validation_base_date_key, value='0000-01-01')
    print(f"xcom put -> demo-target-date:{demo}")
    print(f"xcom put -> reach-curve-target-date:{curve}")
    stored_base_date = task_instance.xcom_pull(key=validation_base_date_key)
    print(f"xcom pull -> base date:{stored_base_date}")


store_date_op = OpTask(op=PythonOperator(task_id=store_dates_task_id, python_callable=store_dates, dag=dag, provide_context=True))
###########################################
#   DAG workflow
###########################################

validation_cluster.add_sequential_body_task(functional_validation)
validation_cluster.add_sequential_body_task(wikipedia_comparison)
validation_cluster.add_sequential_body_task(world_bank_comparison)
validation_cluster.add_sequential_body_task(adbrain_comparison)

gating_cluster.add_sequential_body_task(census_vs_cross_device_comparison_interpretation)
gating_cluster.add_sequential_body_task(adbrain_comparison_interpretation)
gating_cluster.add_sequential_body_task(world_bank_comparison_interpretation)
gating_cluster.add_sequential_body_task(wikipedia_comparison_interpretation)
gating_cluster.add_sequential_body_task(export_master_validated_forecasts)
gating_cluster.add_sequential_body_task(analyze_validated_results)
gating_cluster.add_sequential_body_task(format_validation_results)

maxreach_cluster.add_sequential_body_task(max_reach_setup)

validation_pipeline_dag_pipeline >> store_date_op >> maxreach_cluster >> gating_cluster
store_date_op >> validation_cluster >> gating_cluster
