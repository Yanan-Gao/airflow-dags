from datetime import datetime, timedelta

from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, DEAL_QUALITY_JAR
from dags.dmg.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration
from ttd.ec2.emr_instance_types.memory_optimized.r8g import R8g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.datperf.datasets import platformreport_dataset

job_name = "dmg-deal-quality-decision-power-calculation"
start_date = datetime(2025, 5, 20, 0, 0)
env = TtdEnvFactory.get_from_system()

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1) if env == TtdEnvFactory.prod else None,
    retries=0,
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[DEAL_MANAGEMENT.team.name, "DealQuality"],
    run_only_latest=True
)

dag = ttd_dag.airflow_dag

reAggregateOverLookBackWindow_default = "false"
reAggregateOverLookBackWindow = f"{{{{ dag_run.conf.get('reAggregateOverLookBackWindow', '{reAggregateOverLookBackWindow_default}') }}}}"

ds_date = "{{ (macros.datetime.strptime(dag_run.conf['run_date_time'], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d 00:00:00') if dag_run.run_type == 'manual' else data_interval_start.strftime('%Y-%m-%d 00:00:00')) }}"
runEndExclusive = "{{ ((macros.datetime.strptime(dag_run.conf['run_date_time'], '%Y-%m-%dT%H:%M:%S') + macros.timedelta(days=1)).strftime('%Y-%m-%dT%H:00:00') if dag_run.run_type == 'manual' else data_interval_end.strftime('%Y-%m-%dT%H:00:00')) }}"

# Used in test:
# ds_date = '{{ (data_interval_start - macros.timedelta(days=2)).strftime("%Y-%m-%d 00:00:00") }}'
# runEndExclusive = '{{ (data_interval_start - macros.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00") }}'

check_input_exists = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id="check_input_exists",
        ds_date=ds_date,
        lookback=0,
        poke_interval=60 * 10,
        timeout=60 * 60 * 24,
        datasets=[
            DateGeneratedDataset(
                bucket="ttd-deal-quality",
                path_prefix="DealQualityMetrics",
                data_name="DealAggregatedAvails",
                env_path_configuration=MigratedDatasetPathConfiguration(),
                version=2,
                success_file=None,
            ),
            platformreport_dataset.with_check_type("day")
        ]
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R8g.r8g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R8g.r8g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_4xlarge().cores),
        R8g.r8g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_8xlarge().cores),
        R8g.r8g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_12xlarge().cores),
        R8g.r8g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_16xlarge().cores)
    ],
    on_demand_weighted_capacity=R8g.r8g_16xlarge().cores * 10
)

spark_options = [("conf", "spark.driver.maxResultSize=0"), ("conf", "spark.network.timeout=2400s"),
                 ("conf", "spark.executor.heartbeatInterval=2000s"),
                 ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
                 ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                 ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    retries=0,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": DEAL_MANAGEMENT.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name=job_name + "-job-task",
    retries=0,
    class_name="com.thetradedesk.deals.pipelines.dealquality.decisionpower.AggregateDailyDealDecisionPowerComponents",
    executable_path=get_jar_file_path(DEAL_QUALITY_JAR),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=[("runEndExclusive",
                                        runEndExclusive), ("reAggregateOverLookBackWindow", reAggregateOverLookBackWindow),
                                       ('decisionPowerLookBackWindow', '7'), ("ttd.ds.DailyDealDecisionPowerBaseDataset.isInChain", "true")]
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> check_input_exists >> cluster_task
