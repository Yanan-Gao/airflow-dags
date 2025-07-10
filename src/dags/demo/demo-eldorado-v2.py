import ttd
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime

from ttd.slack import slack_groups

job_schedule_interval = "0 12 * * *"
job_start_date = datetime(2022, 1, 20)

dag = TtdDag(
    dag_id="demo-eldorado-v2",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#scrum-tv-alarms",
    slack_tags=ttd.slack.slack_groups.DEV_SCRUM_DATAPROC,
    tags=["Datalake", "DATAPROC", "Demo"],
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(10)  # Set if cluster requires additional storage
        # .with_ebs_iops(3000)
        # .with_ebs_throughput(125)
        # Default IOPS and throughput are set based on storage size. Customise to optimise for your workload
    ],
    spot_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(10)  # Set if cluster requires additional storage
        # .with_ebs_iops(3000)
        # .with_ebs_throughput(125)
        # Default EBS IOPS and throughput are set based on storage size. Customise to optimise for your workload
    ],
    spot_weighted_capacity=1,
)

cluster_task = EmrClusterTask(
    name="demo-eldorado-v2-test-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.dataproc.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name="demo-eldorado-v2-task",
    class_name="jobs.experiment.DummyWriteJob",
    executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar",
    configure_cluster_automatically=True,
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
