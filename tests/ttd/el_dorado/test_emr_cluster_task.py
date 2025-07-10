import unittest
from datetime import timedelta
from unittest.mock import patch

from airflow.utils.dates import days_ago

from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ec2.emr_instance_types.storage_optimized.i3en import I3en
from ttd.ec2.emr_instance_types.storage_optimized.i4i import I4i
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.allocation_strategies import AllocationStrategyConfiguration, OnDemandStrategy
from ttd.eldorado.base import TtdDag
from ttd.ttdenv import TtdEnvFactory
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask, ClusterVersionNotSupported
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.mixins.tagged_cluster_mixin import TaggedClusterMixin

dag_name = "local-unit-test"
job_slack_channel = "#dev-aifun-alerts"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_large().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_large().with_fleet_weighted_capacity(1)],
    spot_weighted_capacity=1,
)
dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    slack_channel=job_slack_channel,
    retries=1,
    retry_delay=timedelta(minutes=10),
)


class EmrClusterTaskTest(unittest.TestCase):

    def test_cluster_tags(self):
        cluster_version = AwsEmrVersions.AWS_EMR_SPARK_3_2
        cluster = EmrClusterTask(
            name=dag_name,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            cluster_tags={"Team": "AIFUN"},
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            emr_release_label=cluster_version,
            environment=TtdEnvFactory.test,
        )
        dag >> cluster
        expected = [
            {
                "Key": "Environment",
                "Value": TtdEnvFactory.test.execution_env,
            },  # need this for ttd-mlflow #scrum-aifun
            {
                "Key": "Process",
                "Value": dag_name
            },
            {
                "Key": "Resource",
                "Value": "EMR"
            },
            {
                "Key": "Source",
                "Value": "Airflow"
            },
            {
                "Key": "Team",
                "Value": "AIFUN"
            },
            {
                "Key": "Job",
                "Value": dag_name
            },
            {
                "Key": "Service",
                "Value": dag_name
            },
            {
                "Key": "ClusterName",
                "Value": TaggedClusterMixin.get_cluster_name(dag_name, dag_name, 256),
            },
            {
                "Key": "ClusterVersion",
                "Value": cluster_version
            },
            {
                "Key": "Monitoring",
                "Value": "aws_emr"
            }
        ]
        actual = cluster._setup_tasks.last_airflow_op().job_flow_overrides["Tags"]
        self.assertCountEqual(actual, expected)

    def test_constructor__raises_exception_for_cluster_version_below_spark_3_support_threshold(self, ):
        cluster_version = "emr-6.0.0"
        with self.assertRaises(ClusterVersionNotSupported):
            cluster = EmrClusterTask(
                name=dag_name,
                master_fleet_instance_type_configs=master_fleet_instance_type_configs,
                cluster_tags={"Team": "AIFUN"},
                core_fleet_instance_type_configs=core_fleet_instance_type_configs,
                emr_release_label=cluster_version,
            )

    def test_constructor__does_not_raise_exception_for_default_cluster_version(self):
        cluster = EmrClusterTask(
            name=dag_name,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            cluster_tags={"Team": "AIFUN"},
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        )

    def test_constructor__does_not_raise_exception_for_cluster_version_equal_to_spark_3_support_threshold(self, ):
        cluster_version = "emr-6.1.0"
        cluster = EmrClusterTask(
            name=dag_name,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            cluster_tags={"Team": "AIFUN"},
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            emr_release_label=cluster_version,
        )

    def test_constructor__does_not_raise_exception_for_cluster_version_above_spark_3_support_threshold(self, ):
        cluster_version = "emr-7.0.0"
        cluster = EmrClusterTask(
            name=dag_name,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            cluster_tags={"Team": "AIFUN"},
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            emr_release_label=cluster_version,
        )


class TestMutateI3InstanceFleet(unittest.TestCase):

    @patch("airflow.models.Variable.get", return_value=True)
    def test_i3_causes_switch_to_prioritised_allocation_strategy(self, mock_variable_get):
        # Arrange
        original_instance_types = [
            I3.i3_8xlarge().with_fleet_weighted_capacity(1),
            I3.i3_16xlarge().with_fleet_weighted_capacity(2),
        ]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.10.0')

        # Assert
        self.assertDictEqual(
            new_fleet.allocation_strategy.__dict__,
            AllocationStrategyConfiguration(on_demand=OnDemandStrategy.Prioritized).__dict__
        )

    @patch("airflow.models.Variable.get", return_value=True)
    def test_i4i_added_if_all_i3_and_version_6_8_0(self, mock_variable_get):
        # Arrange
        original_instance_types = [I3.i3_8xlarge().with_fleet_weighted_capacity(1)]

        expected_instance_types = [
            I3.i3_8xlarge().with_fleet_weighted_capacity(1).with_priority(3),
            I4i.i4i_8xlarge().with_fleet_weighted_capacity(1).with_priority(2),
        ]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.8.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(expected_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_no_i4_added_if_all_i3_and_version_6_7_0(self, mock_variable_get):
        # Arrange
        original_instance_types = [I3.i3_8xlarge().with_fleet_weighted_capacity(1)]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.7.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(original_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_i3en_instance_fleet_unchanged_if_only_types_requested(self, mock_variable_get):
        # Arrange
        original_instance_types = [I3en.i3en_large().with_fleet_weighted_capacity(1)]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.8.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(original_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_instance_fleet_not_altered_if_none_i3(self, mock_variable_get):
        # Arrange
        original_instance_types = [
            M7g.m7g_xlarge().with_fleet_weighted_capacity(1),
            R5.r5_2xlarge().with_fleet_weighted_capacity(2),
        ]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.8.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self.assertCountEqual(original_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_i3_instances_removed_if_some_i3(self, mock_variable_get):
        # Arrange
        original_instance_types = [
            M7g.m7g_xlarge().with_fleet_weighted_capacity(1),
            I3.i3_2xlarge().with_fleet_weighted_capacity(2),
        ]

        expected_instance_types = [M7g.m7g_xlarge().with_fleet_weighted_capacity(1)]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.11.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(expected_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_i3en_instances_removed_if_some_i3en(self, mock_variable_get):
        # Arrange
        original_instance_types = [
            M7g.m7g_xlarge().with_fleet_weighted_capacity(1),
            I3en.i3en_xlarge().with_fleet_weighted_capacity(2),
        ]

        expected_instance_types = [M7g.m7g_xlarge().with_fleet_weighted_capacity(1)]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.11.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(expected_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_i3en_instances_removed_and_i4_added_if_i3en_and_i3(self, mock_variable_get):
        # Arrange
        original_instance_types = [
            I3.i3_8xlarge().with_fleet_weighted_capacity(1),
            I3en.i3en_xlarge().with_fleet_weighted_capacity(2),
        ]

        expected_instance_types = [
            I3.i3_8xlarge().with_fleet_weighted_capacity(1).with_priority(3),
            I4i.i4i_8xlarge().with_fleet_weighted_capacity(1).with_priority(2),
        ]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.11.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(expected_instance_types, altered_instance_types)

    @patch("airflow.models.Variable.get", return_value=True)
    def test_attributes_preserved_for_new_instances(self, mock_variable_get):
        # Arrange
        original_instance_types = [
            I3.i3_8xlarge().with_fleet_weighted_capacity(1).with_ebs_iops(1000).with_ebs_throughput(10),
        ]

        expected_instance_types = [
            I3.i3_8xlarge().with_fleet_weighted_capacity(1).with_priority(3).with_ebs_iops(1000).with_ebs_throughput(10),
            I4i.i4i_8xlarge().with_fleet_weighted_capacity(1).with_priority(2).with_ebs_iops(1000).with_ebs_throughput(10),
        ]

        fleet = EmrFleetInstanceTypes(instance_types=original_instance_types, on_demand_weighted_capacity=100)

        # Act
        new_fleet = EmrClusterTask._mutate_i3_instance_fleet(fleet, False, 'emr-6.11.0')

        # Assert
        altered_instance_types = new_fleet.instance_types
        self._verify_instance_fleets_are_equivalent(expected_instance_types, altered_instance_types)

    def _verify_instance_fleets_are_equivalent(self, expected: list[EmrInstanceType], actual: list[EmrInstanceType]):
        """
        We can't just provide a __eq__ method on EmrInstanceType because that stops the type from being hashable and there
        exist cases of people creating sets of instance types. We have to serialize somehow.
        """

        def to_dict(obj):
            if obj is None:
                return None
            elif isinstance(obj, (str, int, float, bool)):
                return obj
            elif hasattr(obj, '__dict__'):
                return {key: to_dict(value) for key, value in vars(obj).items()}
            else:
                return str(obj)  # Fallback

        expected_serialized = [to_dict(i) for i in expected]
        actual_serialized = [to_dict(i) for i in actual]

        self.assertCountEqual(expected_serialized, actual_serialized)
