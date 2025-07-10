from datetime import datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
"""
This DAG is to execute spark job OptOutLogConvertJob to convert optout log into parquet,
for diffcache generater use for now.
"""

###########################################
#   Job Configs
###########################################
job_name = 'user-optout-converter'
slack_channel = '#scrum-pdg-alerts'
job_start_date = datetime(2024, 7, 3)
job_schedule_interval = '6 7 * * *'  # Daily at 7:06 am UTC

# general configuration for jobs
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-pdg-assembly.jar"
job_class_name = "com.thetradedesk.jobs.OptOutLogConvertJob"
cluster_name = "optout-log-convert"
pdg_jira_team = "PDG"

aws_region = 'us-east-1'

# master/core instance types for all datasets
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_instance_types = [I3.i3_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(8)]

# pick on demand because of using spot instance may cause it fail randomly.
# one instance is good enough, usually take around 1 min for one day data
# cost is about $1.2
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=core_instance_types,
    on_demand_weighted_capacity=1,
)

spark_options_list = [
    ("conf", "maximizeResourceAllocation=true"),
    # don't let step retry if step fails, fail the job directly for human debugging
    ("conf", "spark.yarn.maxAppAttempts=1")
]
"""
NOTE: depending on the environment you are running this in you will need to change the prefix
to be either env=test (lower environment) or env=prod (for production runs) to a bucket PDG owns
To trigger the job with non default configuration, following json template can be used:
{
  "optout_log_start_date": "2020-05-19",
  "optout_parquet_bucket": "ttd-user-optout",
  "optout_parquet_prefix": "env=prod/guofang.li/optout/parquet/",
  "days": 10
}
"""
job_option_list = [
    (
        'optOutLogStartDate',
        '{{ dag_run.conf.get("optout_log_start_date") if dag_run.conf is not none and dag_run.conf.get("optout_log_start_date") is not none else logical_date.add(days=-1).strftime(\"%Y-%m-%d\") }}'
    ),
    (
        'optOutParquetBucket',
        '{{ dag_run.conf.get("optout_parquet_bucket") if dag_run.conf is not none and dag_run.conf.get("optout_parquet_bucket") is not none else "ttd-user-optout"}}'
    ),
    (
        'optOutParquetPrefix',
        '{{ dag_run.conf.get("optout_parquet_prefix") if dag_run.conf is not none and dag_run.conf.get("optout_parquet_prefix") is not none else "env=prod/optout/user/parquet/"}}'
    ), ('days', '{{ dag_run.conf.get("days") if dag_run.conf is not none and dag_run.conf.get("days") is not none else "1"}}')
]

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_channel,
    tags=["UserOptOutConverter", pdg_jira_team],
)

# this line is need such airflow can find this dag.
adag = dag.airflow_dag

cluster_task = EmrClusterTask(
    name=f"{cluster_name}",
    emr_release_label=emr_release_label,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": "PDG"},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    region_name=aws_region,
    cluster_auto_termination_idle_timeout_seconds=60 * 60,
    retries=0,
)

daily_convert_step = EmrJobTask(
    name="convert",
    class_name=job_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=job_option_list,
    region_name=aws_region,
    retries=0,
)

cluster_task.add_parallel_body_task(daily_convert_step)

final_dag_status = FinalDagStatusCheckOperator(dag=dag.airflow_dag)

dag >> cluster_task

cluster_task.last_airflow_op() >> final_dag_status
