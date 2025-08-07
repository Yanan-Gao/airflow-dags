from datetime import timedelta

from airflow import DAG

from dags.hpc import constants
from dags.hpc.utils import CrossDeviceLevel
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProvider
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask


def get_dataset_check_sensor_task(
    airflow_dag: DAG, dataset: HourGeneratedDataset, cloud_provider: CloudProvider, timeout: int = 6
) -> OpTask:
    """Checks the existence of an upstream dataset."""

    task_id_data_name = dataset.data_name.replace("/", "_")

    return OpTask(
        op=DatasetCheckSensor(
            dag=airflow_dag,
            task_id=f"{str(cloud_provider)}-{task_id_data_name}-check-sensor",
            datasets=[dataset.with_check_type('hour')],
            ds_date="{{ logical_date.to_datetime_string() }}",
            poke_interval=60 * 10,
            cloud_provider=cloud_provider,
            timeout=60 * 60 * timeout,
        )
    )


# TODO (HPC-6367): This should be done in Airflow as opposed to an EMRJobTask.
def get_rotate_generation_ring_cluster(
    cadence_in_hours: int, cross_device_level: CrossDeviceLevel, cardinality_service_host: str, cardinality_service_port: str, aws_jar: str
) -> EmrClusterTask:
    """Gets a cluster that rotates the generation ring."""

    # Create cluster.
    rotate_generation_ring_cluster = EmrClusterTask(
        name='rotate-generation-ring',
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
        custom_java_version=17,
        region_name="us-east-1",
        retries=0
    )

    # Create task.
    eldorado_config_option_pairs_list = [('cadenceInHours', cadence_in_hours), ('cardinalityServiceHost', cardinality_service_host),
                                         ('cardinalityServicePort', cardinality_service_port),
                                         ('crossDeviceLevel', str(cross_device_level))]
    additional_args_option_pairs_list = [('conf', 'spark.yarn.maxAppAttempts=1')]
    rotate_generation_ring_task = EmrJobTask(
        name='rotate-generation-ring-task',
        executable_path=aws_jar,
        class_name='com.thetradedesk.jobs.activecounts.countsredesign.cardinalityservice.RotateGenerationRing',
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
        timeout_timedelta=timedelta(hours=1),
    )
    rotate_generation_ring_cluster.add_sequential_body_task(rotate_generation_ring_task)

    return rotate_generation_ring_cluster
