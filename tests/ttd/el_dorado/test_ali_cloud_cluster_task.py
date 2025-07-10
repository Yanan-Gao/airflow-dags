import unittest

from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.eldorado.alicloud import AliCloudClusterTask
from ttd.ttdenv import TestEnv
from unittest import mock
from ttd.eldorado.aws.emr_cluster_task import ClusterVersionNotSupported

patcher = mock.patch("ttd.ttdenv.TtdEnvFactory.get_from_system", return_value=TestEnv)
patcher.start()

dag_name = "local-unit-test"


class AliCloudClusterTaskTest(unittest.TestCase):

    def test_constructor__raises_exception_for_cluster_version_below_spark_3_support_threshold(self, ):
        cluster_version = "EMR-4.1.3"
        with self.assertRaises(ClusterVersionNotSupported):
            cluster_task = AliCloudClusterTask(
                name="test-ali-cluster",
                master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
                )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
                core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2)
                .with_data_disk_count(4).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
                emr_version=cluster_version,
            )

    def test_constructor__does_not_raise_exception_for_default_cluster_version(self):
        cluster_task = AliCloudClusterTask(
            name="test-ali-cluster",
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
            )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2).with_data_disk_count(4)
            .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
        )

    def test_constructor__does_not_raise_exception_for_cluster_version_equal_to_spark_3_support_threshold(self, ):
        cluster_version = "EMR-5.2.0"
        cluster_task = AliCloudClusterTask(
            name="test-ali-cluster",
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
            )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2).with_data_disk_count(4)
            .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            emr_version=cluster_version,
        )

    def test_constructor__does_not_raise_exception_for_cluster_version_above_spark_3_support_threshold(self, ):
        cluster_version = "EMR-6.12.5"
        cluster_task = AliCloudClusterTask(
            name="test-ali-cluster",
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
            )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2).with_data_disk_count(4)
            .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            emr_version=cluster_version,
        )
