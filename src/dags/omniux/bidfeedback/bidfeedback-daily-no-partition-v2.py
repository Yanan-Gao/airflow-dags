from datetime import datetime, timedelta
from dags.omniux.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

job_name = "bidfeedback-daily-no-partition-v2"
start_date = datetime(2025, 5, 1, 0, 0)
env = TtdEnvFactory.get_from_system()

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    retries=0,
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[OMNIUX.team.name],
    run_only_latest=False,
    max_active_runs=1
)

dag = ttd_dag.airflow_dag

bidfeedback_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='bidfeedback_data_available',
        datasets=[RtbDatalakeDatasource.rtb_bidfeedback_v5.with_check_type('day')],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(1000).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_ebs_size_gb(1000).with_max_ondemand_price().with_fleet_weighted_capacity(M5.m5_8xlarge().cores)],
    on_demand_weighted_capacity=M5.m5_8xlarge().cores * 5
)

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    retries=0,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": OMNIUX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name=job_name + "-job-task",
    retries=0,
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.bidfeedbackv2.BidFeedbackDailySubsetV2Job",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('date', "{{ ds }}")],
    timeout_timedelta=timedelta(hours=8),
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> bidfeedback_sensor >> cluster_task
