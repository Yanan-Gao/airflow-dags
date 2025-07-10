from datetime import datetime, timedelta

from dags.hpc import constants
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_s3_bucket = "ttd-datamarketplace"
job_s3_key = "data-import-aggregator"
job_spark_library_s3_key = job_s3_key + "/jars/prod/data-import-aggregator-emr690.jar"
job_executable_path = f"s3://{job_s3_bucket}/{job_spark_library_s3_key}"
job_log_url = f"s3://{job_s3_bucket}/{job_s3_key}/instance/{{{{ data_interval_start.strftime('%Y%m%d-%H%M%S-%f') }}}}/log/app"
env_str = TtdEnvFactory.get_from_system().execution_env

###########################################
# DAG
###########################################

dag = TtdDag(
    dag_id="ip-data-import-aggregator",
    slack_channel=hpc.alarm_channel,
    dag_tsg="https://thetradedesk.atlassian.net/wiki/spaces/EN/pages/19231846/IPDataImportAggregatorStatus",
    tags=[hpc.jira_team],
    start_date=datetime(2024, 9, 3),
    schedule_interval=timedelta(hours=1),
    dagrun_timeout=timedelta(hours=9),
    max_active_runs=1,
    run_only_latest=True,
)
adag = dag.airflow_dag

###########################################
# IP Data Import Aggregator Cluster Task
###########################################

ip_data_import_aggregator_master_fleet_instance_type_configs = EmrFleetInstanceTypes([
    M5a.m5a_xlarge().with_fleet_weighted_capacity(1),
    M5.m5_xlarge().with_fleet_weighted_capacity(1),
    M5a.m5a_2xlarge().with_fleet_weighted_capacity(1),
    M5.m5_2xlarge().with_fleet_weighted_capacity(1)
],
                                                                                     on_demand_weighted_capacity=1)

ip_data_import_aggregator_core_fleet_instance_type_configs = EmrFleetInstanceTypes([
    M5a.m5a_4xlarge().with_fleet_weighted_capacity(1),
    M5.m5_4xlarge().with_fleet_weighted_capacity(1),
],
                                                                                   on_demand_weighted_capacity=4)

ip_data_import_aggregator_cluster_task = EmrClusterTask(
    name="ip_data_import_aggregator_cluster_task",
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    master_fleet_instance_type_configs=ip_data_import_aggregator_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=ip_data_import_aggregator_core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    enable_prometheus_monitoring=True,
    retries=0
)

###########################################
# IP Data Import Aggregator Job Task
###########################################
ip_data_import_aggregator_job_task_command_line_args = [
    "--logLevel",
    "Info",
    "--sqsQueueUrl",
    "https://sqs.us-east-1.amazonaws.com/003576902480/sns-s3-useast-logs-2-ipdataimportpreagg-collected-queue",
    "--sqsMessageVisibilityTimeout",
    "PT150M",
    "--sqsMinMessageAge",
    "PT0M",
    "--sqsReadMaxNumberOfMessages",
    "10",
    "--sqsReadMaxWaitTime",
    "PT1S",
    "--deleteMessagesAfterProcessing",
    "true",
    "--maxBatchSize",
    "100Gi",
    # The SQS client blocks until the next msg arrives. So setting this to 5 minutes effectively means
    # we accumulate messages for 5 minutes if not reaching max batch size. For now, each hour we only
    # have 3k messages.
    "--maxBatchBuildingTime",
    "PT30S",
    "--maxBatchCount",
    "50",
    "--repartitionFactor",
    "1",
    "--maxInflatedOutputFileSize",
    "110Mi",
    "--logUrl",
    job_log_url,
    "--outputUrl",
    "s3://thetradedesk-useast-logs-2/ipdataimport/collected",
    "--outputType",
    "ipdataimport",
    "--environment",
    env_str,
    "--branch",
    "master",
    "--version",
    "1",
    "--prometheusMetricsEnabled",
    "true",
    "--openTelemetryEndpoint",
    "https://opentelemetry-global-gateway.gen.adsrvr.org/http/v1/metrics"
]

ip_data_import_aggregator_job_task = EmrJobTask(
    name='ip_data_import_aggregator_job_task',
    class_name="com.thetradedesk.dmp.ip_data_import_aggregator.Main",
    cluster_specs=ip_data_import_aggregator_cluster_task.cluster_specs,
    command_line_arguments=ip_data_import_aggregator_job_task_command_line_args,
    executable_path=job_executable_path,
    timeout_timedelta=timedelta(hours=3),
    configure_cluster_automatically=True
)
ip_data_import_aggregator_cluster_task.add_sequential_body_task(ip_data_import_aggregator_job_task)

###########################################
# IP Data Import Aggregator Job Task (DLQ)
###########################################

ip_data_import_aggregator_job_task_command_line_args_dlq = [
    "--logLevel", "Info", "--sqsQueueUrl",
    "https://sqs.us-east-1.amazonaws.com/003576902480/sns-s3-useast-logs-2-ipdataimportpreagg-collected-deadletter",
    "--sqsMessageVisibilityTimeout", "PT20M", "--sqsMinMessageAge", "PT0M", "--sqsReadMaxNumberOfMessages", "10", "--sqsReadMaxWaitTime",
    "PT1S", "--deleteMessagesAfterProcessing", "true", "--maxBatchSize", "100Gi", "--maxBatchBuildingTime", "PT30S", "--maxBatchCount",
    "50", "--repartitionFactor", "1", "--maxInflatedOutputFileSize", "110Mi", "--logUrl", job_log_url, "--outputUrl",
    "s3://thetradedesk-useast-logs-2/ipdataimport/collected", "--outputType", "ipdataimport", "--environment", env_str, "--branch",
    "master", "--version", "1", "--prometheusMetricsEnabled", "true", "--openTelemetryEndpoint",
    "https://opentelemetry-global-gateway.gen.adsrvr.org/http/v1/metrics"
]

ip_data_import_aggregator_job_task_dlq = EmrJobTask(
    name='ip_data_import_aggregator_job_task_dlq',
    class_name="com.thetradedesk.dmp.ip_data_import_aggregator.Main",
    cluster_specs=ip_data_import_aggregator_cluster_task.cluster_specs,
    command_line_arguments=ip_data_import_aggregator_job_task_command_line_args_dlq,
    executable_path=job_executable_path,
    timeout_timedelta=timedelta(hours=3),
    configure_cluster_automatically=True
)
ip_data_import_aggregator_cluster_task.add_sequential_body_task(ip_data_import_aggregator_job_task_dlq)

###########################################
# Dependencies
###########################################
dag >> ip_data_import_aggregator_cluster_task >> OpTask(op=FinalDagStatusCheckOperator(dag=adag))
