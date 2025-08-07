from datetime import datetime

from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
import dags.hpc.constants as constants
from ttd.slack.slack_groups import hpc

start_date = datetime(2024, 11, 18, 12)
dag_name = 'counts-sampled-avails-etl-alicloud'
cadence_in_hours = 1

# Prod Variables
schedule = f'0 */{cadence_in_hours} * * *'
el_dorado_jar_url = constants.HPC_ALI_EL_DORADO_JAR_URL

# # Test Variables
# schedule = None
# el_dorado_jar_url = 'oss://ttd-build-artefacts/eldorado/mergerequests/dgs-HPC-5425-migrate-sampled-avails-etl/latest/eldorado-hpc-assembly.jar'

####################################################################################################################
# Dag
####################################################################################################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/AQACG',
    tags=[hpc.jira_team],
    max_active_runs=5,
    depends_on_past=True,
    run_only_latest=False,
)
adag = dag.airflow_dag

####################################################################################################################
# Cluster
####################################################################################################################
cluster_name = 'counts-sampled-avails-etl-cluster'

cluster = AliCloudClusterTask(
    name=cluster_name,
    master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_2X(
    )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_3X()).with_node_count(4).with_data_disk_count(1)
    .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

####################################################################################################################
# Task
####################################################################################################################
task_name = 'counts-sampled-avails-etl-task'
class_name = 'com.thetradedesk.jobs.activecounts.datacollection.sampledavails.SampledAvailsETL'

java_settings_list = [
    ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"),
    ("runTime", "{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
    ("protoNumPartitions", "10000"),
    ('ttd.ds.default.storageProvider', 'alicloud'),
]

spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC")]

task = AliCloudJobTask(
    name=task_name,
    class_name=class_name,
    cluster_spec=cluster.cluster_specs,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list,
    jar_path=el_dorado_jar_url,
    configure_cluster_automatically=True,
    command_line_arguments=['--version']
)

cluster.add_parallel_body_task(task)

####################################################################################################################
# Dependencies
####################################################################################################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> cluster >> final_dag_check
