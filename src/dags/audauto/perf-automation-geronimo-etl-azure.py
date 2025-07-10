from datetime import datetime, timedelta

from dags.dataproc.datalake.datalake_parquet_utils import CloudJobConfig
from datasources.datasources import Datasources
from ttd.ec2.cluster_params import ClusterParams
from ttd.operators.final_dag_status_check_operator import (
    FinalDagStatusCheckOperator,
)
from ttd.tasks.op import OpTask
from ttd.cloud_provider import CloudProviders
from ttd.slack.slack_groups import AUDAUTO
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

import copy
import logging

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.

job_config = CloudJobConfig(
    CloudProviders.azure,
    capacity=32,
    metrics_capacity=2,
    disks_per_hdi_vm=1,
    output_files=100,
    hdi_permanent_cluster=False,
)


def get_spark_args(params: ClusterParams):
    spark_args = params.to_spark_arguments()
    args = [(key, str(value)) for key, value in spark_args['args'].items()]
    conf_args = [("conf", f"{key}={value}") for key, value in spark_args['conf_args'].items()]
    return conf_args + args


spark_options_list = get_spark_args(HDIInstanceTypes.Standard_E64_v3().calc_cluster_params(job_config.capacity))

# Jar
GERONIMO_JAR_AZURE = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/libs/geronimo/jars/prod/geronimo.jar"

# Date functions based on the execution date for triggering the sensors:

# In Airflow, the run timestamped with a given date-time only starts when the period that it covers ends.
# Meaning, the job_schedule_interval parameter below, currently set to 1 hour, defines that a job with
# execution_date 2023-10-01 00:00 will only start after 2023-10-01 01:00.
execution_date = "{{data_interval_start.strftime('%Y-%m-%d')}}"
execution_date_Ymd = "{{data_interval_start.strftime('%Y%m%d')}}"
execution_date_H = "{{data_interval_start.strftime('%H')}}"
contextual_sensor_date = "{{(data_interval_start + macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}}"
logfile_sensor_hour = ("{{(data_interval_start + macros.timedelta(hours=1)).strftime('%Y/%m/%d/%H')}}")

# Number of bidfeedback hours to be joined with one hour of bid requests
bf_hours = 2

logging.info(f"job_config.provider: ${job_config.provider}, clusterService: ${job_config.cluster_service}")

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
geronimo_etl_dag = TtdDag(
    dag_id="perf-automation-geronimo-etl-azure",
    start_date=datetime(2024, 7, 24),
    schedule_interval=timedelta(hours=1),
    max_active_runs=1,
    dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Geronimo+ETL+TSG",
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=["AUDAUTO", "DATPERF"],
    slack_channel="#perf-auto-audiences-alerts",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    default_args={"owner": "AUDAUTO"},
)
dag = geronimo_etl_dag.airflow_dag

azure_dataset_check_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="upstream_dataset_check_azure",
        datasets=[
            Datasources.rtb_datalake.rtb_bidrequest_v5.as_read(),
            Datasources.rtb_datalake.rtb_bidfeedback_v5.as_read(),
        ],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,  # in seconds
        timeout=60 * 60 * 12,  # wait up to 12 hours
        cloud_provider=job_config.provider,
    )
)

aws_dataset_check_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="upstream_dataset_check_aws",
        datasets=[
            Datasources.contextual.contextual_requested_urls.with_check_type("hour").as_read(),
            Datasources.perfauto.eligible_user_data_processed.with_check_type("hour")
        ],
        ds_date=contextual_sensor_date,
        poke_interval=60 * 10,  # in seconds
        timeout=60 * 60 * 12,  # wait up to 12 hours
        cloud_provider=CloudProviders.aws,
    )
)

brbf_etl_cluster = HDIClusterTask(
    name="geronimo-job-cluster",
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_E32_v3(),
        workernode_type=HDIInstanceTypes.Standard_E64_v3(),
        num_workernode=job_config.capacity,
        disks_per_node=job_config.disks_per_hdi_vm,
    ),
    cluster_tags={
        "Cloud": job_config.provider.__str__(),
        "Team": AUDAUTO.team.jira_team,
    },
    permanent_cluster=job_config.hdi_permanent_cluster,
    cluster_version=HDIClusterVersions.AzureHdiSpark33,
    enable_openlineage=False,
)

brbf_step_1 = HDIJobTask(
    name="geronimo-brbf-job",
    jar_path=GERONIMO_JAR_AZURE,
    class_name="job.BrBf",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=java_settings_list + [
        ("date", execution_date),
        ("hour", execution_date_H),
        ("numberOfBfHours", bf_hours),
        ("writePartitions", "600"),
        ("addSeenInBiddingData", "false"),
        ("cloudProvider", "azure"),
        ("ttd.azure.enable", "true"),
        ("openlineage.enable", "false"),
        ("ttd.s3.access.enable", "true"),
        ("azure.key", "eastusttdlogs,ttdmlplatform"),
    ],
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
)
brbf_etl_cluster.add_parallel_body_task(brbf_step_1)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

(geronimo_etl_dag >> azure_dataset_check_sensor >> aws_dataset_check_sensor >> brbf_etl_cluster >> final_dag_status_step)
