from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.pipeline_config import PipelineConfig
from datasources.datasources import Datasources
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_name = "path-optimization-experiment-performance"
start_date = datetime(2025, 1, 8, 6, 0)
env = TtdEnvFactory.get_from_system()

ttdDag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    enable_slack_alert=True,
    tags=["OmnichannelRetargeting"],
    depends_on_past=True  # because the cumulative job adds to previous day's data
)

dag: DAG = ttdDag.airflow_dag
# Overriding jar path to eldorado, but still using AwsEmrVersions.AWS_EMR_SPARK_3_5
config = PipelineConfig(jar="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar")
report_date = '{{ (data_interval_start).strftime(\"%Y-%m-%d\") }}'

lookbacksInDays = {"7Days": "7", "14Days": "14", "30Days": "30"}

# LogWorkflow config
logworkflow_connection_open_gate = "lwdb" if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "sandbox-lwdb"
log_start_time = '{{ data_interval_start.strftime("%Y-%m-%d %H:00:00") }}'

# 0. Wait for the day's RTB report to be complete

# RTB Platform Reports
platform_report_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

# Late RTB Platform Reports
late_platform_report_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportLateDataPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

platform_report_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='platform_report_data_available',
        datasets=[platform_report_dataset, late_platform_report_dataset],
        # will check all 24 hours
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 8 hours
        timeout=60 * 60 * 8,
    )
)

check_bidfeedback_data_task = OpTask(
    op=DatasetCheckSensor(
        task_id="check_bidfeedback_data",
        datasets=[Datasources.ctv.bidfeedback_daily_subset_no_partition(env.dataset_read_env)],
        # checks last hour of the day
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 3 hours
        timeout=60 * 60 * 3,
    )
)

# 1. Path optimization cumulative KPI aggregation
kpi_agg_cluster = EmrClusterTask(
    name='omnichannel-group-kpi-cumulative-agg',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.SixteenX, instance_capacity=10),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)

cumulative_agg_task = EmrJobTask(
    name="PathOptimizationRTBReportCumulativeAgg",
    class_name="com.thetradedesk.jobs.pathoptimizationexperiment.RTBReportCumulativeAgg",
    executable_path=config.jar,
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=config.get_step_additional_configurations(),
    eldorado_config_option_pairs_list=[("date", report_date), ("rootPath", "s3://ttd-omnichannel-optimizations"),
                                       ("ttd.ds.RTBReportCumulativeAggDataSet.isInChain", "true"), ("decayFactor", "1"),
                                       ("secondaryConversionWeight", "0"), ("clickWeight", "0"), ("saveDailyIncrement", "true")]
)
kpi_agg_cluster.add_parallel_body_task(cumulative_agg_task)

# 2. Pre-aggregate bidfeedback into daily distinct households and channels reached
daily_hh_channel_reach_cluster = EmrClusterTask(
    name='daily-hh-channels-reach',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, instance_capacity=15),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)

daily_hh_channel_reach_task = EmrJobTask(
    name="DailyHhChannelsReach",
    class_name="com.thetradedesk.jobs.pathoptimizationexperiment.DailyHhChannelsReach",
    executable_path=config.jar,
    timeout_timedelta=timedelta(hours=1),
    additional_args_option_pairs_list=config.get_step_additional_configurations(),
    eldorado_config_option_pairs_list=[("date", report_date)]
)
daily_hh_channel_reach_cluster.add_parallel_body_task(daily_hh_channel_reach_task)

# 3. Omnichannel group level reach KPI in lookbackday windows
reach_agg_cluster = EmrClusterTask(
    name='omnichannel-group-reach-agg',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, instance_capacity=3),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)

for lookback, days in lookbacksInDays.items():
    reach_agg_task = EmrJobTask(
        name="ChannelReachedKpiModel_" + lookback,
        class_name="com.thetradedesk.jobs.pathoptimizationexperiment.OmnichannelHhReachKpiModel",
        executable_path=config.jar,
        timeout_timedelta=timedelta(minutes=40),
        additional_args_option_pairs_list=config.get_step_additional_configurations(),
        eldorado_config_option_pairs_list=[
            ("date", report_date),
            ("lookbackDays", days),  # date is inclusive
            ("ttd.ds.BidFeedbackHhChannelReachDataSet.isInChain", "true")
        ]
    )
    reach_agg_cluster.add_parallel_body_task(reach_agg_task)

# 4. Experiment result
experiment_cluster = EmrClusterTask(
    name='path-optimization-exeriment',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, instance_capacity=1),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
for lookback, days in lookbacksInDays.items():
    experiment_task_incremental = EmrJobTask(
        name="PathOptimizationExperiment_" + lookback,
        class_name="com.thetradedesk.jobs.pathoptimizationexperiment.PathOptimizationExperiment",
        executable_path=config.jar,
        timeout_timedelta=timedelta(minutes=40),
        additional_args_option_pairs_list=config.get_step_additional_configurations(),
        eldorado_config_option_pairs_list=[("date", report_date), ("minImpressionCountOmnichannel", "1000"), ("minSpendOmnichannel", "100"),
                                           ("minDaysActive", "5"), ("ttd.ds.OmnichannelHhReachDataSet.isInChain", "true"),
                                           ("lookbackDays", days), ("ttd.ds.RTBReportDailyIncrementalAggDataSet.isInChain", "true"),
                                           ("useIncremental", "true")]
    )
    experiment_cluster.add_parallel_body_task(experiment_task_incremental)

experiment_task_ag_cpgn_goal = EmrJobTask(
    name="PathOptimizationExperiment_AdGroupGoal",
    class_name="com.thetradedesk.jobs.pathoptimizationexperiment.PathOptimizationExperimentByAdGroup",
    executable_path=config.jar,
    timeout_timedelta=timedelta(minutes=30),
    additional_args_option_pairs_list=config.get_step_additional_configurations(),
    eldorado_config_option_pairs_list=[("date", report_date)]
)
experiment_cluster.add_parallel_body_task(experiment_task_ag_cpgn_goal)

# 5. Report kpi outliers. Currently only look at the 30 days lookback ones
outliers_cluster = EmrClusterTask(
    name='path-optimization-exeriment-report-outliers',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, instance_capacity=1),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
outliers_task = EmrJobTask(
    name="PathOptimizationExperimentStats",
    class_name="com.thetradedesk.jobs.pathoptimizationexperiment.PathOptimizationExperimentStats",
    executable_path=config.jar,
    timeout_timedelta=timedelta(minutes=40),
    additional_args_option_pairs_list=config.get_step_additional_configurations(),
    eldorado_config_option_pairs_list=[("date", report_date), ("lookbackDays", 30), ("controlSplitRatio", 0.05),
                                       ("controlSplitRatioThreshold", 0.005), ("cpmThreshold", 0.05), ("ctrThreshold", 0.05),
                                       ("convRateThreshold", 0.05), ("cpaThreshold", 0.05)]
)
outliers_cluster.add_parallel_body_task(outliers_task)

# 6. Merge datasets for all lookback windows, apply filters, and save the result to the export path.
export_cluster = EmrClusterTask(
    name='path-optimization-exeriment-report-export',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, instance_capacity=1),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
export_task = EmrJobTask(
    name="PathOptimizationExperimentExport",
    class_name="com.thetradedesk.jobs.pathoptimizationexperiment.PathOptimizationExperimentExport",
    executable_path=config.jar,
    timeout_timedelta=timedelta(minutes=30),
    additional_args_option_pairs_list=config.get_step_additional_configurations(),
    eldorado_config_option_pairs_list=[("date", report_date), ("lookbackOptions", ",".join(lookbacksInDays.values())),
                                       ("outputFileCount", "10"), ("ttd.ds.CpgnGroupPathOptExperimentOutputDataSet.isInChain", "true")]
)
export_cluster.add_parallel_body_task(export_task)

logworkflow_open_sql_import_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'gatingType': 2000607,  # dbo.fn_Enum_GatingType_ImportOmnichannelGroupPathOptimizationPerformance()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': log_start_time
            }
        },
        task_id="logworkflow_open_sql_import_gate",
    )
)

ttdDag >> platform_report_sensor >> kpi_agg_cluster >> experiment_cluster
ttdDag >> check_bidfeedback_data_task >> daily_hh_channel_reach_cluster >> reach_agg_cluster >> experiment_cluster
experiment_cluster >> outliers_cluster >> export_cluster >> logworkflow_open_sql_import_gate
