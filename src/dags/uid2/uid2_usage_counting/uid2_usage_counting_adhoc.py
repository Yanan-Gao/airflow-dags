from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory

from ttd.slack import slack_groups

env = TtdEnvFactory.get_from_system()

job_name = "uid2-usage-counting-adhoc"
job_schedule_interval = None
job_start_date = datetime(2025, 2, 10)

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#prj-uid2-publisher-usage-report",
    retries=3,
    retry_delay=timedelta(minutes=5),
    tags=[slack_groups.UID2.team.name],
)
adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_8xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_8xlarge().with_ebs_size_gb(1024)],
    on_demand_weighted_capacity=96 * 10,
)

cluster_task = EmrClusterTask(
    name=f'{job_name}-cluster',
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.UID2.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    log_uri="s3://thetradedesk-uid2/env=prod/uid2usagelogs/"
    if env.execution_env == 'prod' else "s3://thetradedesk-uid2/env=test/uid2usagelogs/"
)

v2_usage_processing_task = EmrJobTask(
    name=f'{job_name}-v2-task',
    class_name="jobs.uid2usagetracking.Uid2UsageProcessingV2",
    executable_path=
    "{{dag_run.conf.get('JarPath', 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-uid2-assembly.jar')}}",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ('Uid2UsageLogS3RootPath', 'thetradedesk-useast-logs-2/uid2usagecapturedata/collected'),
        (
            'Uid2UsageReportS3RootPath',
            "thetradedesk-bi/uid2usage/{{'' if dag_run.conf.get('WriteToDailyReportFolder', 'False') == 'True' else 'adhoc/'}}v2"
        ),
        (
            'OutputPath',
            '{{dag_run.conf.get("CustomPrefix", "")}}daily-report-{{ macros.datetime.strptime(dag_run.conf.get("CaptureDate"), "%Y-%m-%d").strftime("%Y%m%d") }}'
        ),
        ('CaptureFromTime', "{{ dag_run.conf.get('CaptureDate') }}T00:00:00"),
    ],
)

v3_usage_processing_task = EmrJobTask(
    name=f'{job_name}-v3-task',
    class_name="jobs.uid2usagetracking.Uid2UsageProcessingV3",
    executable_path=
    "{{dag_run.conf.get('JarPath', 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-uid2-assembly.jar')}}",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ('Uid2UsageLogS3RootPath', 'thetradedesk-useast-logs-2/uid2usagecapturedata/collected'),
        (
            'Uid2UsageReportS3RootPath',
            "thetradedesk-bi/uid2usage/{{'' if dag_run.conf.get('WriteToDailyReportFolder', 'False') == 'True' else 'adhoc/'}}v3"
        ),
        (
            'OutputPath',
            '{{dag_run.conf.get("CustomPrefix", "")}}daily-report-{{ macros.datetime.strptime(dag_run.conf.get("CaptureDate"), "%Y-%m-%d").strftime("%Y%m%d")}}'
        ),
        ('CaptureFromTime', "{{ dag_run.conf.get('CaptureDate') }}T00:00:00"),
    ],
    timeout_timedelta=timedelta(hours=6),
)

cluster_task.add_sequential_body_task(v2_usage_processing_task)
cluster_task.add_sequential_body_task(v3_usage_processing_task)

dag >> cluster_task
