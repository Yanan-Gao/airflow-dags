from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.utils import CrossDeviceLevel
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

###
# Variables
###
# General Variables
start_date = datetime(2025, 4, 20, 1, 0)
cadence_in_hours = 24
schedule = '0 1 * * *'

# Prod Variables
run_only_latest = True
end_date = None
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL
cardinality_service_host = constants.CARDINALITY_SERVICE_PROD_HOST
cardinality_service_port = constants.CARDINALITY_SERVICE_PROD_PORT

# # Test Variables
# run_only_latest = False
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/sjh-HPC-7013-generate-overlaps-dataset/latest/eldorado-hpc-assembly.jar"
# cardinality_service_host = constants.CARDINALITY_SERVICE_TEST_HOST


def get_relevance_overlaps_export_cluster() -> EmrClusterTask:
    """Gets a Cluster that exports overlaps."""

    # Create cluster.
    relevance_overlaps_export_cluster = EmrClusterTask(
        name='counts-relevance-overlaps-export-cluster',
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                M7g.m7g_2xlarge().with_fleet_weighted_capacity(1),
            ],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                M7g.m7g_2xlarge().with_fleet_weighted_capacity(2),
                M7g.m7g_4xlarge().with_fleet_weighted_capacity(4),
            ],
            on_demand_weighted_capacity=4,
        ),
        cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        retries=0
    )

    # Create task.
    eldorado_config_option_pairs_list = [('datetime', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                         ('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port), ('innerParallelism', 8),
                                         ('requestTimeoutSeconds', 600), ('crossDeviceLevel', str(CrossDeviceLevel.DEVICE)),
                                         ("storageProvider", str(CloudProviders.aws))]

    relevance_overlaps_export_task = EmrJobTask(
        name="counts-relevance-overlaps-export-task",
        executable_path=aws_jar,
        class_name="com.thetradedesk.jobs.countsexport.RelevanceOverlapsExport",
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
        timeout_timedelta=timedelta(hours=18),
        configure_cluster_automatically=True,
    )
    relevance_overlaps_export_cluster.add_sequential_body_task(relevance_overlaps_export_task)

    return relevance_overlaps_export_cluster


# Build DAG

dag = TtdDag(
    dag_id="counts-relevance-overlaps-export",
    start_date=start_date,
    end_date=end_date,
    schedule_interval=schedule,
    run_only_latest=run_only_latest,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/AoBjGw',
    tags=[hpc.jira_team],
    retries=0
)
airflow_dag = dag.airflow_dag

###
# Steps
###

# Relevance Overlaps Export
relevance_overlaps_export = get_relevance_overlaps_export_cluster()

# Check DAG status.
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=airflow_dag))

###
# Dependencies
###
dag >> relevance_overlaps_export >> final_dag_check
