from datetime import timedelta, datetime

from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.eldorado.base import TtdDag
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import PSR

dag_name = "cpm-lookup-table"

# Jar Path
JAR_PATH = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/ds/libs/markets-ds-assembly.jar"

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2023, 5, 9, 9, 0),  # Dag start time(UTC), note that if job_schedule_interval is a timedelta,
    # the first run will be start_date+timedelta, not immediately at start_date
    schedule_interval='0 15 * * 2',  # weekly on Tuesday at 15:00 UTC (8AM PT)
    depends_on_past=False,  # The current execution does not depend on any previous execution.
    run_only_latest=True,  # If we miss previous weeks, we don't care, just run the current one
    max_active_runs=1,
    retries=2,  # auto retry twice in the next two day
    retry_delay=timedelta(days=1),
    tags=["PSR"],
    slack_channel="#scrum-psr-alerts",
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

task_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=12,
)

cluster = EmrClusterTask(
    name="cpm-lookup-table",
    cluster_tags={'Team': PSR.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    task_fleet_instance_type_configs=task_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=60 * 60,
)

####################################################################################################################
# steps
####################################################################################################################

cpm_etl_task = EmrJobTask(
    name="CPMETL",
    class_name="com.thetradedesk.ds.libs.CPMLookup.CPMETL",
    executable_path=JAR_PATH,
    timeout_timedelta=timedelta(hours=2),
)

lookup_table_generation = EmrJobTask(
    name="CPMLookupTableGeneration",
    class_name="com.thetradedesk.ds.libs.CPMLookup.CPMLookupTableGeneration",
    executable_path=JAR_PATH,
    timeout_timedelta=timedelta(hours=2),
)

cpm_etl_task >> lookup_table_generation

cluster.add_parallel_body_task(cpm_etl_task)
cluster.add_parallel_body_task(lookup_table_generation)

dag >> cluster

adag = dag.airflow_dag
