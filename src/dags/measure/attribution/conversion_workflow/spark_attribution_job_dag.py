from datetime import datetime
from typing import List, Dict

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from ttd.cloud_provider import CloudProviders
from ttd.eldorado.xcom.helpers import get_push_xcom_task_id
from dags.measure.attribution.databricks_config import DatabricksConfig, get_databricks_env_param, \
    create_attribution_dag
from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.task_config import RunCondition
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow, DatabricksRegion
from ttd.tasks.op import OpTask

DEFAULT_DEPENDENCY_LOOKBACK_IN_DAYS = 7
DEFAULT_MAX_BATCH_SIZE = 2000000000

DEPENDENCY_LOOKBACK_IN_DAYS = f'{{{{ dag_run.conf.get("dependency_lookback_in_days") if dag_run.conf is not none and dag_run.conf.get("dependency_lookback_in_days") is not none else "{DEFAULT_DEPENDENCY_LOOKBACK_IN_DAYS}" }}}}'
MAX_BATCH_SIZE = f'{{{{ dag_run.conf.get("max_batch_size") if dag_run.conf is not none and dag_run.conf.get("max_batch_size") is not none else "{DEFAULT_MAX_BATCH_SIZE}" }}}}'


def create_dag(env: str, databricks_region: DatabricksRegion):
    ttd_dag: TtdDag = create_attribution_dag(
        dag_name="job_executor",
        cloud_provider=CloudProviders.aws,
        start_date=datetime(2025, 6, 3, 0, 0),
        schedule_interval="*/10 */4 * * *"
    )

    scheduler_task_id = "attribution_schedule"
    attribution_readiness_check_task_id = "attribution_readiness"
    attribution_workflow_task_id = "attribution_workflow"
    do_nothing_task_id = "attribution_skip"
    scheduler_job_name = "attribution_scheduler"
    scheduler_push_xcom_task_id = get_push_xcom_task_id(scheduler_job_name)

    run_scheduler_task = DatabricksWorkflow(
        job_name=scheduler_task_id,
        cluster_name="measure_spark_attribution_scheduler_cluster",
        cluster_tags={
            **DatabricksConfig.BASE_TAG,
            **DatabricksConfig.ATTRIBUTION_JOB_TAG,
        },
        databricks_spark_version=DatabricksConfig.DATABRICKS_VERSION,
        driver_node_type="r6gd.2xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=2,
        region=databricks_region,
        retries=0,
        spark_configs=DatabricksConfig.SPARK_CONF_JOB_SCHEDULER,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.schedule.AttributionSchedulerJob",
                executable_path=DatabricksConfig.ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH,
                job_name=scheduler_job_name,
                additional_command_line_parameters=
                [f"envParam={env}", f"dependencyLookbackInDays={DEPENDENCY_LOOKBACK_IN_DAYS}", f"maxBatchSize={MAX_BATCH_SIZE}"],
                do_xcom_push=True
            )
        ]
    )

    def check_attribution_dependency_readiness(**kwargs):
        notebook_output = kwargs["ti"].xcom_pull(task_ids=scheduler_push_xcom_task_id)

        if isinstance(notebook_output, dict) and isinstance(notebook_output.get('result'), list):
            if notebook_output['result'] and len(notebook_output["result"]) > 0:
                job_param = notebook_output["result"][0]
                kwargs["ti"].xcom_push(key="SessionId", value=job_param["SessionId"])
                kwargs["ti"].xcom_push(key="SessionTimeUtc", value=job_param["SessionTimeUtc"])
                kwargs["ti"].xcom_push(key="JobIntervalStart", value=job_param["JobIntervalStart"])
                kwargs["ti"].xcom_push(key="JobIntervalEnd", value=job_param["JobIntervalEnd"])
                kwargs["ti"].xcom_push(key="Bucketed_WeeklyIndex_LastEntryDate", value=job_param["Bucketed_WeeklyIndex_LastEntryDate"])
                kwargs["ti"].xcom_push(key="Bucketed_DailyIndex_LastEntryDate", value=job_param["Bucketed_DailyIndex_LastEntryDate"])
                return attribution_workflow_task_id
            else:
                return do_nothing_task_id
        else:
            raise ValueError("Malformed or unexpected structure in the scheduler job json output")

    check_attribution_dependency_readiness_task = OpTask(
        op=BranchPythonOperator(
            task_id=attribution_readiness_check_task_id, python_callable=check_attribution_dependency_readiness, provide_context=True
        )
    )

    do_nothing_task = OpTask(op=EmptyOperator(task_id=do_nothing_task_id))

    attribution_steps_params = [
        f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='JobIntervalStart') }}}}",
        f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='JobIntervalEnd') }}}}",
        f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='SessionId') }}}}",
        f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='SessionTimeUtc') }}}}",
        f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='Bucketed_DailyIndex_LastEntryDate') }}}}",
        f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='Bucketed_WeeklyIndex_LastEntryDate') }}}}", env
    ]

    attribution_workflow_start = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.schedule.SessionUpdater",
        executable_path=DatabricksConfig.ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH,
        job_name="start",
        additional_command_line_parameters=[
            env, f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='SessionId') }}}}", "Processing",
            "{{ '{{job.run_id}}' }}"
        ],
    )

    attribution_tasks = create_attribution_tasks(DatabricksConfig.ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH, attribution_steps_params)
    attribution_tasks['attribution_setting'].depends_on = [attribution_workflow_start]

    attribution_workflow_fail = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.schedule.SessionUpdater",
        executable_path=DatabricksConfig.ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH,
        job_name="fail",
        additional_command_line_parameters=
        [env, f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='SessionId') }}}}", "Failed"],
        depends_on=[attribution_tasks['attributed_event_output']],
        run_condition=RunCondition.AT_LEAST_ONE_FAILED
    )

    attribution_workflow_succeed = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.schedule.SessionUpdater",
        executable_path=DatabricksConfig.ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH,
        job_name="succeed",
        additional_command_line_parameters=
        [env, f"{{{{ ti.xcom_pull(task_ids='{attribution_readiness_check_task_id}', key='SessionId') }}}}", "Success"],
        depends_on=[attribution_tasks['attributed_event_output']],
    )

    attribution_workflow = DatabricksWorkflow(
        job_name=attribution_workflow_task_id,
        cluster_name="measure_spark_attribution_scheduler_cluster",
        cluster_tags={
            **DatabricksConfig.BASE_TAG,
            **DatabricksConfig.ATTRIBUTION_JOB_TAG
        },
        databricks_spark_version=DatabricksConfig.DATABRICKS_VERSION,
        use_photon=True,
        driver_node_type="r6gd.12xlarge",
        worker_node_type="r6gd.8xlarge",
        worker_node_count=32,
        spark_configs=DatabricksConfig.SPARK_CONF_ATTRIBUTION_PHOTON,
        region=databricks_region,
        retries=0,
        tasks=[attribution_workflow_start, *attribution_tasks.values(), attribution_workflow_fail, attribution_workflow_succeed]
    )

    ttd_dag >> run_scheduler_task >> check_attribution_dependency_readiness_task >> [attribution_workflow, do_nothing_task]
    return ttd_dag.airflow_dag


def create_attribution_tasks(executable_path: str, task_params: List[str]) -> Dict[str, SparkDatabricksTask]:
    attribution_workflow_attribution_setting = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.AttributionSettingTask",
        executable_path=executable_path,
        job_name="attribution_setting",
        additional_command_line_parameters=task_params
    )

    attribution_workflow_vendor_event_tuple = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.VendorAttributionTask",
        executable_path=executable_path,
        job_name="vendor_event_tuple",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_attribution_setting],
    )

    attribution_workflow_identity_event_tuple = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.IdentityAttributionTask",
        executable_path=executable_path,
        job_name="identity_event_tuple",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_attribution_setting],
    )

    attribution_workflow_conversion_deduplication = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.ConversionDeduplicationTask",
        executable_path=executable_path,
        job_name="conversion_deduplication",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_identity_event_tuple],
    )

    attribution_workflow_bidfeedback_hydration = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.BidFeedbackHydrationTask",
        executable_path=executable_path,
        job_name="bidfeedback_hydration",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_conversion_deduplication, attribution_workflow_vendor_event_tuple],
    )

    attribution_workflow_bidfeedback_data_element_hydration = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.BidFeedbackDataElementHydrationTask",
        executable_path=executable_path,
        job_name="bidfeedback_data_element_hydration",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_conversion_deduplication, attribution_workflow_vendor_event_tuple],
    )

    attribution_workflow_retail_sales_index = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.RetailSalesIndexTask",
        executable_path=executable_path,
        job_name="retail_sales_index",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_conversion_deduplication, attribution_workflow_vendor_event_tuple],
    )

    attribution_workflow_credit_distribution = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.CreditDistributionTask",
        executable_path=executable_path,
        job_name="credit_distribution",
        additional_command_line_parameters=task_params,
        depends_on=[attribution_workflow_retail_sales_index],
    )

    attribution_workflow_attributed_event_output = SparkDatabricksTask(
        class_name="com.thetradedesk.attribution.dagtask.AttributedEventTableTask",
        executable_path=executable_path,
        job_name="attributed_event_output",
        additional_command_line_parameters=task_params,
        depends_on=[
            attribution_workflow_bidfeedback_hydration, attribution_workflow_bidfeedback_data_element_hydration,
            attribution_workflow_credit_distribution
        ],
    )

    return {
        t.name: t
        for t in [
            attribution_workflow_attribution_setting, attribution_workflow_vendor_event_tuple, attribution_workflow_identity_event_tuple,
            attribution_workflow_conversion_deduplication, attribution_workflow_bidfeedback_hydration,
            attribution_workflow_bidfeedback_data_element_hydration, attribution_workflow_retail_sales_index,
            attribution_workflow_credit_distribution, attribution_workflow_attributed_event_output
        ]
    }


attribution_job_dag = create_dag(env=get_databricks_env_param(), databricks_region=DatabricksRegion.use())
