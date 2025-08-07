from datetime import timedelta

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState

from dags.tv.constants import FORECAST_JAR_PATH
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import pdg
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask

forecasting_task_name = "dsr-delete-forecasting-agg-data"


def create_forecasting_data_task_group(uuids: str) -> ChainOfTasks:

    aggregate_step_sensor = OpTask(
        op=ExternalTaskSensor(
            task_id="check-for-completion",
            external_dag_id="uf-columnstore-v2_no_xd_level-data-generator",
            external_task_id="uf_columnstore_v2_no_xd_level_avails_aggregation_cluster",
            allowed_states=[TaskInstanceState.FAILED, TaskInstanceState.SUCCESS],
            poll_interval=60,
            execution_delta=timedelta(hours=14)
        ),
        task_id="check-for-running-forecasting-agg-steps"
    )

    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_4xlarge().with_fleet_weighted_capacity(16),
            R6gd.r6gd_8xlarge().with_fleet_weighted_capacity(32),
            R6gd.r6gd_12xlarge().with_fleet_weighted_capacity(48),
            R6gd.r6gd_16xlarge().with_fleet_weighted_capacity(64),
        ],
        on_demand_weighted_capacity=960
    )

    cluster = EmrClusterTask(
        name=forecasting_task_name,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        enable_prometheus_monitoring=True,
        maximize_resource_allocation=True,
        cluster_tags={"Team": pdg.jira_team}
    )

    step = EmrJobTask(
        name="forecasting-agg-data",
        class_name="com.thetradedesk.etlforecastjobs.preprocessing.columnstore.dsdr.DeletePIIFromAggregates",
        executable_path=FORECAST_JAR_PATH,
        eldorado_config_option_pairs_list=[("uuids", uuids)],
        maximize_resource_allocation=True,
    )

    cluster.add_sequential_body_task(step)

    return ChainOfTasks("datagov-dsr-delete-forecasting-data", [aggregate_step_sensor, cluster])
