from datetime import datetime, timedelta

from dags.cmo.utils.fleet_batch_config import getMasterFleetInstances, EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances
from dags.omniux.utils import get_jar_file_path
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_name = "bidfeedback-daily-no-partition"
start_date = datetime(2025, 6, 9, 0, 0)
env = TtdEnvFactory.get_from_system()

spark_options_list = [("executor-memory", "27G"), ("driver-memory", "23G"), ("driver-cores", "16"), ("executor-cores", "8"),
                      ("conf", "spark.kryoserializer.buffer.max=2047m"), ("conf", "spark.driver.maxResultSize=0"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.executor.memoryOverhead=16g"), ("conf", "spark.sql.shuffle.partitions=12800"),
                      ("conf", "spark.default.parallelism=12800"), ("conf", "spark.network.timeout=720s"),
                      ("conf", "spark.executor.heartbeatInterval=30s")]

bidfeedback_subset_job_options = [('date', "{{ ds }}")]

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

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    retries=0,
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX),
    cluster_tags={"Team": OMNIUX.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=30),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name=job_name + "-job-task",
    retries=0,
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.bidfeedback.BidFeedbackDailySubset",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=bidfeedback_subset_job_options,
    timeout_timedelta=timedelta(hours=8),
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> bidfeedback_sensor >> cluster_task
