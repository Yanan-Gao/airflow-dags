from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.slack.slack_groups import FORECAST
from dags.forecast.validation.history_service.history_service_rpcs import retrieve_active_deployments_with_addresses
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

dag_name = "live-adgroup-validation-ts"

# Environment
env = TtdEnvFactory.get_from_system()

###############################################################################
# DAG
###############################################################################

# The top-level dag
live_adgroup_validation_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime.now() - timedelta(hours=3),
    schedule_interval="*/7 * * * *",  # Running every 7 minutes
    retries=0,
    retry_delay=timedelta(minutes=1),
    max_active_runs=1,
    slack_alert_only_for_prod=True,
    tags=[FORECAST.team.jira_team],
    slack_tags=FORECAST.team.jira_team,
    slack_channel="#dev-forecasting-validation-alerts",
    enable_slack_alert=True
)

dag = live_adgroup_validation_dag.airflow_dag

# def upload_active_experiments_to_s3():
#     ttd_env_string = TtdEnvFactory.get_from_system().execution_env
#     retrieve_active_experiments(ttd_env=ttd_env_string, should_upload_to_cloud_storage=True, should_return_results=False)
#     return True
#
#
# upload_forecasts_with_audience_to_s3_task = OpTask(
#     op=PythonOperator(
#         task_id='get-active-experiments-from-history-service',
#         dag=dag,
#         python_callable=upload_active_experiments_to_s3,
#         provide_context=True
#     )
# )
#
#
# def upload_production_deployments_to_s3():
#     ttd_env_string = TtdEnvFactory.get_from_system().execution_env
#     retrieve_production_deployments(ttd_env=ttd_env_string, should_upload_to_cloud_storage=True, should_return_results=False)
#     return True
#
#
# upload_production_deployments_to_s3_task = OpTask(
#     op=PythonOperator(
#         task_id='get-production-deployments-from-history-service',
#         dag=dag,
#         python_callable=upload_production_deployments_to_s3,
#         provide_context=True
#     )
# )


def upload_active_deployments_with_addresses_to_s3():
    ttd_env_string = TtdEnvFactory.get_from_system().execution_env
    retrieve_active_deployments_with_addresses(ttd_env=ttd_env_string, should_upload_to_cloud_storage=True, should_return_results=False)
    return True


upload_active_deployments_with_addresses_to_s3_task = OpTask(
    op=PythonOperator(
        task_id='get-active-deployments-from-history-service',
        dag=dag,
        python_callable=upload_active_deployments_with_addresses_to_s3,
        provide_context=True
    )
)

live_adgroup_validation_task = OpTask(
    op=TaskServiceOperator(
        task_name="AdGroupForecastValidationTask",
        task_config_name="AdGroupForecastValidationTaskConfig",
        scrum_team=FORECAST.team,
        retries=3,
        resources=TaskServicePodResources.large(),
        telnet_commands=[
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension1 65536",  # GrapeshotPredictsCategoryDimensionDescriptor
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension1 137438953472",  # IntegralContextualCategoryDimensionDescriptor
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension1 274877906944",  # Peer39ContextualCategoryDimensionDescriptor
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension1 549755813888",  # GrapeshotContextualCategoryDimensionDescriptor
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension1 1099511627776",  # DoubleVerifyContextualCategoryDimensionDescriptor
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension2 16",  # TTDContextualCategoryDimensionDescriptor
            "invoke RamDataMapperHelper.UserSampledSupportQuery.EnableDimension2 268435456"  # PreBidContextualCategoryDimensionDescriptor
        ],
        # branch_name="release-2024.12.03",
    )
)

# DAG dependencies
live_adgroup_validation_dag >> upload_active_deployments_with_addresses_to_s3_task >> live_adgroup_validation_task
