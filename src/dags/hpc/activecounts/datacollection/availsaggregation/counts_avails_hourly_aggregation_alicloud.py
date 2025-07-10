from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDatasources
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'counts-avails-hourly-aggregation-alicloud'
cadence_in_hours = 1
max_expected_delay = 3

# Prod Variables
job_start_date = datetime(2024, 7, 14, 1, 0)
schedule = f'0 */{cadence_in_hours} * * *'
ali_jar = constants.HPC_ALI_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# ali_jar = 'oss://ttd-build-artefacts/eldorado/mergerequests/sjh-HPC-4967-migrate-data-collection-alicloud-spark3/latest/eldorado-hpc-assembly.jar'

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/AQACG',
    max_active_runs=5,
    depends_on_past=True,
    run_only_latest=False,
    default_args={"wait_for_downstream": True},
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag

###########################################
# Check Dependencies
###########################################

check_avails_upstream_completed = OpTask(
    op=DatasetCheckSensor(
        datasets=[CountsDatasources.avails_7_day_alicloud.with_check_type(check_type="hour")],
        ds_date='{{data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}',
        poke_interval=int(timedelta(hours=1).total_seconds()),
        timeout=int(timedelta(hours=24).total_seconds()),
        task_id='avails_dataset_check',
        cloud_provider=CloudProviders.ali,
    )
)

###########################################
# Steps
###########################################

# Avail Aggregation Cluster

avail_hourly_aggregation_cluster_name = 'counts-avails-hourly-aggregation'

avail_hourly_aggregation_cluster = AliCloudClusterTask(
    name=avail_hourly_aggregation_cluster_name,
    master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
    )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_2X()).with_node_count(2).with_data_disk_count(1)
    .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS
)

# Avail Aggregation Step

avail_hourly_aggregation_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.availsaggregation.usersampledavailaggregation.UserSampledAvailsHourlyAggregation'
avail_hourly_aggregation_step_job_name = 'avails-hourly-aggregation'

avail_hourly_aggregation_step = AliCloudJobTask(
    name=avail_hourly_aggregation_step_job_name,
    class_name=avail_hourly_aggregation_step_spark_class_name,
    jar_path=ali_jar,
    eldorado_config_option_pairs_list=[('datetime', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                       ('cadenceInHours', max_expected_delay), ("ttd.ds.default.storageProvider", "alicloud")],
    configure_cluster_automatically=True,
    command_line_arguments=['--version'],
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
)

avail_hourly_aggregation_cluster.add_parallel_body_task(avail_hourly_aggregation_step)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> check_avails_upstream_completed >> avail_hourly_aggregation_cluster >> final_dag_check
