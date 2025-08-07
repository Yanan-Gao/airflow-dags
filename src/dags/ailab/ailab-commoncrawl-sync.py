from datetime import datetime
from datetime import timedelta

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes

job_schedule_interval = "0 */12 * * *"
job_start_date = datetime(2024, 7, 7)
date_str = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/")
source_location_uri = f"s3://commoncrawl/crawl-data/CC-NEWS/{date_str}"
destination_location_uri = "s3://ttd-ailab-omniscience-useast/env=prod/commoncrawl/data/raw/"

name = "ailab-commoncrawl-sync"

dag = TtdDag(
    dag_id=name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#prj-ailab-omniscience",
    tags=["Omniscience", "AILAB"],
    depends_on_past=False,
    run_only_latest=True,
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_2xlarge().with_fleet_weighted_capacity(1),
                    M5a.m5a_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_2xlarge().with_fleet_weighted_capacity(1),
                    M5a.m5a_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

cluster_task = EmrClusterTask(
    name=name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": "AILAB"},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name="omniscience-commoncrawl-sync",
    class_name=None,
    configure_cluster_automatically=True,
    executable_path="aws",
    command_line_arguments=["s3", "sync", f"{source_location_uri}", f"{destination_location_uri}"],
    timeout_timedelta=timedelta(hours=1)
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
