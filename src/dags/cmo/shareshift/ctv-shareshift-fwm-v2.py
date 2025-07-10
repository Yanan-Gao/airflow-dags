"""
Monthly job to process FWM impression, demographics data, and generate weights and corrections
for ShareShift ltv prediction
"""
from airflow import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from dags.cmo.utils.pipeline_config import PipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.idnt.identity_helpers import DagHelpers
from datasources.datasources import Datasources
from datasources.sources.fwm_datasources import FwmExternalDatasource
from datasources.sources.shareshift_datasources import ShareshiftDatasource, ShareshiftDataType
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
"""
  Compared to v1, this dag
    1. removed HH sampling (the app can handle more data)
    2. uses FWM firehose data and handles both national and local impressions differently
"""

pipeline_name = "ctv-shareshift-fwm-v2"
job_start_date = datetime(2024, 7, 15, 0, 0)
job_schedule_interval = "0 15 15 * *"  # Runs 15:00 on the 15th of each month (FWM data is delayed for 14 days)
pipeline_config = PipelineConfig()
env = TtdEnvFactory.get_from_system()
"""
 Configurable values
"""
country = Country.US
provider = ACRProviders.Fwm

shareshift_dataset = ShareshiftDatasource(country, provider)
shareshift_config = [
    ("country", country),
    ("provider", provider),
    # monthly job: on the 15th of a month, trigger a run w/ logical_date being 15 of previous month
    ("startDate", '{{ logical_date.replace(day=1).strftime(\"%Y-%m-%d\") }}'),
    ("endDate", '{{ (logical_date + macros.timedelta(days=31)).replace(day=1).strftime(\"%Y-%m-%d\") }}'),
    ("shareshiftPath", shareshift_dataset.get_shareshift_dataset(ShareshiftDataType.Impression, version=1).get_root_path())
]

athena_output_location = 's3://thetradedesk-useast-data-import/linear/acr/athena-output'

dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["fwm", "shareshift"],
    retries=1,
    retry_delay=timedelta(hours=1)
)
adag: DAG = dag.airflow_dag

################################################################################################
# Steps
################################################################################################


def getEmrClusterTask(name, master, worker, read_from_test=False):
    return EmrClusterTask(
        name=name + f"-{provider}-{country}",
        master_fleet_instance_type_configs=master,
        cluster_tags={"Team": CMO.team.jira_team},
        core_fleet_instance_type_configs=worker,
        emr_release_label=pipeline_config.emr_release_label,
        additional_application_configurations=pipeline_config.get_cluster_additional_configurations(),
        enable_prometheus_monitoring=True,
        environment=TtdEnvFactory.test if read_from_test else TtdEnvFactory.get_from_system()
    )


# <editor-fold desc="Impressions generation section">
"""
 Impressions, networks generation. National and Local
"""
imps_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
imps_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=10)
imps_cluster_task = getEmrClusterTask("shareshift-imps-networks-cluster", imps_master_cluster, imps_worker_cluster)

imps_job_task = EmrJobTask(
    name='ShareShift-FWM-impressions-networks',
    class_name="jobs.ctv.shareshift.LinearImpDemoAggregation",
    eldorado_config_option_pairs_list=shareshift_config +
    [("geoGraphPath", "s3://ttd-ctv/shareshift/env=prod/geo-enriched-open-graph-owdi-person-demo/graph"),
     ("ttd.GraphHouseholdMatchingDataSet.isInChain", "true")],
    additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations() +
    [("conf", "spark.sql.shuffle.partitions=1780")],
    timeout_timedelta=timedelta(minutes=120),
    executable_path=pipeline_config.jar,
    cluster_specs=imps_cluster_task.cluster_specs
)

imps_cluster_task.add_parallel_body_task(imps_job_task)

# </editor-fold>

# before we run demographics, confirm receipt the latest demographics data generation date
wait_for_demo_data = DagHelpers.check_datasets(
    [FwmExternalDatasource.fwm_demographics],
    """{{ (logical_date + macros.timedelta(days=31)).replace(day=1).strftime(\"%Y-%m-%d %H:%M:%S\") }}""",
    poke_minutes=240,  # check every 4 hours
    timeout_hours=24  # for up to 1 days
)

# <editor-fold desc="Weight generation section">
"""
 Weights generation
"""
demo_path = shareshift_dataset.get_full_s3_path(ShareshiftDataType.Demographic, pipeline_config.env.dataset_write_env, version=2) \
            + "/monthdate={{ logical_date.strftime(\"%Y%m\") }}"
output_path_prefix = shareshift_dataset.get_full_s3_path(ShareshiftDataType.Weight, pipeline_config.env.dataset_write_env, version=2)


def weight_local_kubernetes_pod(local_level: str):
    return TtdKubernetesPodOperator(
        namespace='linear-tv-data',
        image='production.docker.adsrvr.org/ttd/ctv/python-script-runner:2830923',
        name=f"ShareShift-FWM-weight-{local_level}",
        task_id=f"shareshift_weight-{local_level}",
        dnspolicy='ClusterFirst',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=adag,
        service_account_name='linear-tv-data',
        startup_timeout_seconds=800,
        log_events_on_failure=True,
        cmds=["python"],
        arguments=[
            "./executor_shareshift.py", "--program=weight", f"--country={country}", f"--observed_demo_path={demo_path}",
            f"--output_path={output_path_prefix}/level={local_level}/monthdate={{{{ logical_date.strftime(\"%Y%m\") }}}}/part-weights.parquet",
            f"--local_level={local_level}"
        ],
        env_vars=dict(PROMETHEUS_ENV=env.execution_env),
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='2G', limit_memory='4G', request_cpu='2')
    )


metro_weight_job = weight_local_kubernetes_pod("metro")
region_weight_job = weight_local_kubernetes_pod("region")
metro_weight_job_task = OpTask(op=metro_weight_job)
region_weight_job_task = OpTask(op=region_weight_job)

# </editor-fold>

# <editor-fold desc="Correction adjustment section">
"""
 Correction model to adjusts prediction, only for National
"""
corrections_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX)
corrections_worker_cluster = getFleetInstances(
    EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=3
)  # runtime doesn't improve with increasing instances

corrections_cluster_task = getEmrClusterTask("shareshift-corrections-cluster", corrections_master_cluster, corrections_worker_cluster)

corrections_job_task = EmrJobTask(
    name='ShareShift-FWM-corrections',
    class_name="jobs.ctv.shareshift.Corrections",
    eldorado_config_option_pairs_list=shareshift_config + [("uniqueHhOnNetwork", 60000), ("uniqueHhOnAdvertiser", 600000),
                                                           ("uniqueNetworkOnAdvertiser", 20), ("uniqueHhOnPopularAdvertiser", 200000)],
    additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations() + [("conf", "spark.sql.shuffle.partitions=450")],
    timeout_timedelta=timedelta(minutes=200),
    executable_path=pipeline_config.jar,
    cluster_specs=corrections_cluster_task.cluster_specs
)

corrections_cluster_task.add_parallel_body_task(corrections_job_task)

# </editor-fold>

# <editor-fold desc="Opt out HH section">
"""
 Optout: filter out HHs that are no longer in Experian crosswalk
"""
optout_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
optout_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=5)

optout_cluster_task = getEmrClusterTask("shareshift-optout-cluster", optout_master_cluster, optout_worker_cluster)

optout_job_task = EmrJobTask(
    name='ShareShift-FWM-optout',
    class_name="jobs.ctv.shareshift.OptOutHandler",
    eldorado_config_option_pairs_list=shareshift_config +
    [("crosswalkOutputPath", Datasources.experian().crosswalk_output(provider).get_root_path()), ("numOfMonthsToOptOut", "13")],
    additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations(),
    timeout_timedelta=timedelta(minutes=120),
    executable_path=pipeline_config.jar,
    cluster_specs=optout_cluster_task.cluster_specs
)

optout_cluster_task.add_parallel_body_task(optout_job_task)

# </editor-fold>
"""
 TODO: add a data checker before we update athena table. Currently use a dummy placeholder
"""
data_checker = OpTask(op=DummyOperator(task_id="data_checker", dag=adag))

# <editor-fold desc="Athena table partition refresh">
"""
 Athena table partition refresh
"""


def refreshAthenaTableTask(table_name: str):
    return OpTask(
        op=AWSAthenaOperator(
            task_id=f'update_athena_partitioned_table_{table_name}',
            output_location=athena_output_location,
            query=f'MSCK REPAIR TABLE {table_name};',
            retries=3,
            sleep_time=60,
            aws_conn_id='aws_default',
            database='ctv_db',
            dag=adag,
            trigger_rule='none_failed'
        )
    )


# Note: each refresh task is set up as the downstream of data checker here
data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_impressions_national")
data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_impressions_local_metro")
data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_impressions_local_region")

data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_networks_national")
data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_networks_local")

data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_demographics")

data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_weights")
data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_weights_region")

data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_corrections_national")

data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_filter_input_local_metro")
data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_filter_input_local_region")

data_checker >> refreshAthenaTableTask("prod_shareshift_us_fwm_vertical_mapping")

# </editor-fold>

dag >> wait_for_demo_data >> imps_cluster_task >> metro_weight_job_task >> region_weight_job_task
region_weight_job_task >> corrections_cluster_task >> optout_cluster_task >> data_checker
