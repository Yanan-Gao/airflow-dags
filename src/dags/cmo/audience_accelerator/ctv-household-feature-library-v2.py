"""
Weekly job to process CTV bidrequests, OWDI demographic and user profiles to generate household level
features that powers household-level prediction models (e.g. light tv viewer).
"""
from airflow import DAG
from airflow.operators.weekday import BranchDayOfWeekOperator, WeekDay
from datetime import datetime, timedelta

from dags.cmo.utils.constants import Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.pipeline_config import PipelineConfig
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################
pipeline_name = "ctv-feature-library-us-v2"
job_start_date = datetime(2025, 4, 14, 0, 0)
job_schedule_interval = "0 10 * * *"
country = Country.US
pipeline_config = PipelineConfig()

################################################################################################
# DAG
################################################################################################
dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["ctv", "feature-library"],
    retries=0,
    max_active_runs=3,
    run_only_latest=False
)
adag: DAG = dag.airflow_dag

check_bidrequest_data_task = OpTask(
    op=DatasetCheckSensor(
        dag=adag,
        datasets=[RtbDatalakeDatasource.rtb_bidrequest_v5],
        lookback=1,
        task_id="check_bidrequest_data",
        ds_date="{{ data_interval_start.to_datetime_string() }}",
        poke_interval=60 * 60 * 3,
        timeout=60 * 60 * 48,
        depends_on_past=True
    )
)

br_cluster_task = EmrClusterTask(
    name=f"ctv-bidrequest-preprocess-{country}-cluster",
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=13),
    emr_release_label=pipeline_config.emr_release_label,
    additional_application_configurations=pipeline_config.get_cluster_additional_configurations(),
    enable_prometheus_monitoring=True,
)

br_job_task = EmrJobTask(
    name=f"ctv-bidrequest-preprocess-{country}-job",
    class_name="jobs.ctv.household_feature_library.DailyFilterBidRequest",
    eldorado_config_option_pairs_list=[("country", country), ("date", '{{ data_interval_start | ds }}')],
    additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations(),
    timeout_timedelta=timedelta(hours=5),
    executable_path=pipeline_config.jar
)
br_cluster_task.add_parallel_body_task(br_job_task)

cluster_task = EmrClusterTask(
    name=f"ctv-feature-library-{country}-cluster",
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=40),
    emr_release_label=pipeline_config.emr_release_label,
    additional_application_configurations=pipeline_config.get_cluster_additional_configurations(),
    enable_prometheus_monitoring=True,
)

job_task = EmrJobTask(
    name=f"ctv-feature-library-{country}-job",
    class_name="jobs.ctv.household_feature_library.HouseholdFeatureLibraryJob",
    eldorado_config_option_pairs_list=[
        ("openlineage.enable", "false"),
        ("country", country),
        ("endDate", '{{ data_interval_end | ds }}'),  # endDate is exclusive, so we use data_interval_end
        (
            "output_path", "s3://ttd-ctv/household-feature-library/" + country + "/" + pipeline_config.env.dataset_write_env +
            "/HouseholdFeatureLibraryV2/date=" + '{{ data_interval_end | ds_nodash }}'
        )
    ] + [("ttd.ds.FilteredTvFeatureBidRequestDataset.isInChain", "true")],
    additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations(),
    timeout_timedelta=timedelta(hours=3),
    executable_path=pipeline_config.jar,
    cluster_specs=cluster_task.cluster_specs
)
cluster_task.add_parallel_body_task(job_task)

run_ctv_feature_branch = OpTask(
    op=BranchDayOfWeekOperator(
        week_day=[WeekDay.MONDAY],
        task_id='run_ctv_feature_branch',
        follow_task_ids_if_true=[cluster_task.task_id],
        follow_task_ids_if_false=[],
        use_task_logical_date=True,
        dag=adag
    )
)

dag >> check_bidrequest_data_task >> br_cluster_task >> run_ctv_feature_branch >> cluster_task
