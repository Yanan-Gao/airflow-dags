import unittest

from ttd.ec2.cluster_params import calc_cluster_params
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5


class ClusterParamsTest(unittest.TestCase):

    def test_calc_returns_initial_params_as_provided(self):
        print("test_calc_returns_initial_params_as_provided")
        params = calc_cluster_params(10, 16, 64)
        self.assertEqual(params.instances, 10)
        self.assertEqual(params.vcores, 16)
        self.assertEqual(params.node_memory, 64)
        self.assertEqual(params.parallelism, 290)
        self.assertEqual(params.partitions, 290)

    def test_calc_returns_reasonable_params(self):
        print("test_calc_returns_reasonable_params")
        params = calc_cluster_params(2, 8, 32)
        self.assertEqual(params.executor_instances, 3)
        self.assertEqual(params.executor_cores, 3)
        self.assertEqual(params.parallelism, 18)
        self.assertEqual(params.partitions, 18)
        self.assertAlmostEqual(params.executor_memory / 1024, 11, 0)
        self.assertAlmostEqual(params.executor_memory_overhead / 1024, 1.2, 1)

    def test_calc_returns_correct_memory_with_unit(self):
        print("test_calc_returns_correct_memory_with_unit")
        params = calc_cluster_params(2, 8, 32)
        self.assertEqual(params.executor_memory.__str__() + "m", params.executor_memory_with_unit)
        self.assertEqual(
            params.executor_memory_overhead.__str__() + "m",
            params.executor_memory_overhead_with_unit,
        )

    def test_calc_use_provided_parallelism_factor(self):
        print("test_calc_use_provided_parallelism_factor")
        params = calc_cluster_params(2, 8, 32, parallelism_factor=10)
        self.assertEqual(params.parallelism, 90)

    def test_calc_use_provided_parallelism_factor_override(self):
        print("test_calc_use_provided_parallelism_factor_override")
        params = M5.m5_2xlarge().calc_cluster_params(instances=2, parallelism_factor=10)
        self.assertEqual(params.parallelism, 90)

    def test_calc_use_provided_partitions(self):
        print("test_calc_use_provided_partitions")
        params = calc_cluster_params(2, 8, 32, partitions=2)
        self.assertEqual(params.partitions, 2)
        self.assertEqual(
            params.to_spark_arguments().get("conf_args").get("spark.sql.shuffle.partitions"),
            2,
        )

    def test_unsupported_instance_type(self):
        print("test_unsupported_instance_type")
        with self.assertRaises(Exception) as context:
            R5.r5_large().calc_cluster_params(instances=1)

        self.assertTrue("Instance Type Not Supported" in str(context.exception))

    def test_calc_max_core_override(self):
        print("test_calc_max_core_override")
        params = R5.r5_xlarge().calc_cluster_params(instances=1, max_cores_executor=1)
        self.assertEqual(params.parallelism, 4)
