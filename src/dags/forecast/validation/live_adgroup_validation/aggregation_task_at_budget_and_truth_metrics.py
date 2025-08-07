# from datetime import datetime
# import json
#
# import logging
# from airflow.operators.python import PythonOperator
# from dags.forecast.validation.validation_helpers.run_db_queries import run_insertions_and_query
# import pandas as pd
# from dags.forecast.validation.validation_helpers.aws_helper import save_json_to_s3
#
# from dags.forecast.validation.constants import FORECAST_VALIDATION_JOBS_PATH, FORECAST_VALIDATION_JOBS_WHL_NAME
# from dags.forecast.validation.validation_helpers.truth_metrics_helper import \
#     get_high_confidence_adgroups_batched_strings
# from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
# from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
# from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
# from ttd.eldorado.aws.emr_pyspark import S3PysparkEmrTask
# from ttd.eldorado.base import TtdDag
#
# from ttd.eldorado.databricks.tasks.notebook_databricks_task import NotebookDatabricksTask
# from ttd.eldorado.databricks.workflow import DatabricksWorkflow
# from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
# from ttd.tasks.op import OpTask
# from ttd.ttdenv import TtdEnvFactory
# from ttd.slack.slack_groups import FORECAST
#
# job_start_date = datetime(2025, 5, 12)
# input_date = '{{ dag_run.conf.get("input_date", data_interval_start.strftime("%Y-%m-%d")) }}'
# # By default, output_date is set to input_date
# output_date = '{{ dag_run.conf.get("output_date", dag_run.conf.get("input_date", data_interval_start.strftime("%Y-%m-%d"))) }}'
# ttd_env = TtdEnvFactory.get_from_system()
#
# # version = "latest"
# # whl_path = f"{FORECAST_VALIDATION_JOBS_PATH}/release/{version}/pyspark/{FORECAST_VALIDATION_JOBS_WHL_NAME}"
# # aggregate_results_and_truth_path = f"{FORECAST_VALIDATION_JOBS_PATH}/release/{version}/pyspark/aggregate_result_and_truth/aggregate_results_and_truth_job.py"
#
# # paths for testing:
# branch_name = "lgr-FORECAST-6266-add-external-win-rate-notebook"
# whl_path = f"{FORECAST_VALIDATION_JOBS_PATH}/dev/{branch_name}/pyspark/{FORECAST_VALIDATION_JOBS_WHL_NAME}"
# aggregate_results_and_truth_path = f"{FORECAST_VALIDATION_JOBS_PATH}/dev/{branch_name}/pyspark/aggregate_result_and_truth/aggregate_results_and_truth_job.py"
#
# emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_5
# python_version = "3.11.10"
#
# master_fleet_instance_type_configs = EmrFleetInstanceTypes(
#     instance_types=[
#         R7gd.r7gd_2xlarge().with_fleet_weighted_capacity(1),
#         R7gd.r7gd_4xlarge().with_fleet_weighted_capacity(1),
#     ],
#     on_demand_weighted_capacity=1
# )
# core_fleet_instance_type_configs = EmrFleetInstanceTypes(
#     instance_types=[
#         R7gd.r7gd_2xlarge().with_fleet_weighted_capacity(1),
#         R7gd.r7gd_4xlarge().with_fleet_weighted_capacity(2),
#         R7gd.r7gd_8xlarge().with_fleet_weighted_capacity(4),
#     ],
#     on_demand_weighted_capacity=8
# )
#
# # DAG Creation
# dag_name = 'aggregate-truth-metrics-for-activity-stable'
#
# ttd_dag = TtdDag(
#     dag_id=dag_name,
#     start_date=job_start_date,
#     schedule_interval="0 14 * * *",  # Once a day, two hours after generation new lag set
#     tags=[FORECAST.team.jira_team],
#     slack_tags=FORECAST.team.jira_team,
#     slack_channel="#dev-forecasting-validation-alerts",
#     max_active_runs=1,
# )
# attd_dag = ttd_dag.airflow_dag
#
# cluster_task = EmrClusterTask(
#     name=dag_name,
#     master_fleet_instance_type_configs=master_fleet_instance_type_configs,
#     cluster_tags={"Team": FORECAST.team.jira_team},
#     core_fleet_instance_type_configs=core_fleet_instance_type_configs,
#     cluster_auto_terminates=False,
#     emr_release_label=emr_version,
#     whls_to_install=[whl_path],
#     python_version=python_version,
# )
#
# arguments = [f"--ttd_env={ttd_env.execution_env}", f"--date={input_date}", f"--output_date={output_date}"]
#
# aggregate_results_and_truth_task = S3PysparkEmrTask(
#     name="aggregate-at-budget-results-and-truth-emr",
#     entry_point_path=aggregate_results_and_truth_path,
#     additional_args_option_pairs_list=[("conf", "spark.sql.execution.arrow.pyspark.enabled=true"), ("conf", "spark.driver.memory=16G"),
#                                        ("conf", "spark.driver.cores=8")],
#     cluster_specs=cluster_task.cluster_specs,
#     command_line_arguments=arguments,
# )
#
# cluster_task.add_parallel_body_task(aggregate_results_and_truth_task)
#
# job_name = 'aggregate-results-and-potential-truth'
# bid_request_derived_notebook_path = "/Workspace/Users/lucian.rusu@thetradedesk.com/forecasting-validation-etls/databricks_notebooks/BidRequestDerivedAggNotebook"
# external_win_rate_notebook_path = "/Workspace/Users/lucian.rusu@thetradedesk.com/forecasting-validation-etls/databricks_notebooks/ExternalWinRateNotebook"
# internal_win_rate_notebook_path = "/Workspace/Users/lucian.rusu@thetradedesk.com/forecasting-validation-etls/databricks_notebooks/InternalWinRateNotebook"
# databricks_task = DatabricksWorkflow(
#     job_name=job_name,
#     cluster_name=f"{job_name}_cluster",
#     worker_node_type=R7gd.r7gd_2xlarge().instance_name,
#     worker_node_count=12,
#     cluster_tags={"Team": FORECAST.team.jira_team},
#     databricks_spark_version="15.3.x-scala2.12",
#     tasks=[
#         NotebookDatabricksTask(
#             job_name='bid-request-derived-agg',
#             notebook_path=bid_request_derived_notebook_path,
#             whl_paths=[whl_path],
#             notebook_params={
#                 "logical_date": "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}",
#                 "ttd_env": ttd_env.execution_env,
#             },
#         ),
#         NotebookDatabricksTask(
#             job_name='external_win_rate',
#             notebook_path=external_win_rate_notebook_path,
#             whl_paths=[whl_path],
#             notebook_params={
#                 "logical_date": "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}",
#             },
#         ),
#         NotebookDatabricksTask(
#             job_name='internal_win_rate',
#             notebook_path=internal_win_rate_notebook_path,
#             whl_paths=[whl_path],
#             notebook_params={
#                 "logical_date": "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}",
#             },
#         ),
#     ],
# )
#
# ttd_dag >> cluster_task
# ttd_dag >> databricks_task
