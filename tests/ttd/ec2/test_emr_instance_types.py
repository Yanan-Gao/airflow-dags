import unittest

from ttd.ec2.cluster_params import Defaults
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5


class EmrInstanceTypesTest(unittest.TestCase):

    def test_r5_xlarge_returns_correct_instance_type(self):
        print("test_r5_xlarge_returns_correct_instance_type")
        r5xlarge = R5.r5_xlarge()
        self.assertEqual(r5xlarge.instance_name, "r5.xlarge")
        self.assertEqual(r5xlarge.with_bid_price(35).market_type, "SPOT")

    def test_r5_xlarge_returns_correct_machine_spec(self):
        print("test_r5_xlarge_returns_correct_machine_spec")
        r5xlarge = R5.r5_xlarge()
        self.assertEqual(r5xlarge.cores, 4)
        self.assertEqual(r5xlarge.memory, 32)

    def test_calc_ebs_iops__returns_explicit_iops(self):
        ebs_size = 100
        ebs_iops = Defaults.MAX_GP3_IOPS
        instance_type = (M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size).with_ebs_iops(ebs_iops))
        result = instance_type._calc_ebs_iops()
        self.assertEqual(ebs_iops, result)

    def test_calc_ebs_iops__returns_default_iops_below_threshold(self):
        ebs_size = Defaults.GP3_IOPS_CALC_THRESHOLD_GIB - 100
        ebs_iops = Defaults.GP3_IOPS
        instance_type = M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size)
        result = instance_type._calc_ebs_iops()
        self.assertEqual(ebs_iops, result)

    def test_calc_ebs_iops__calculates_iops_above_threshold_below_max(self):
        ebs_size = Defaults.GP3_IOPS_CALC_THRESHOLD_GIB + 100
        ebs_iops = ebs_size * Defaults.IOPS_PER_GIB_MULTIPLIER
        instance_type = M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size)
        result = instance_type._calc_ebs_iops()
        self.assertEqual(ebs_iops, result)

    def test_calc_ebs_iops__caps_iops_at_max_for_large_volumes(self):
        ebs_size = Defaults.MAX_GP3_IOPS / Defaults.IOPS_PER_GIB_MULTIPLIER + 100
        ebs_iops = Defaults.MAX_GP3_IOPS
        instance_type = M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size)
        result = instance_type._calc_ebs_iops()
        self.assertEqual(ebs_iops, result)

    def test_calc_ebs_throughput__returns_explicit_throughput(self):
        ebs_size = 100
        ebs_throughput = Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC
        instance_type = (M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size).with_ebs_throughput(ebs_throughput))
        result = instance_type._calc_ebs_throughput()
        self.assertEqual(ebs_throughput, result)

    def test_calc_ebs_throughput__returns_default_throughput_below_threshold(self):
        ebs_size = Defaults.GP3_THROUGHPUT_CALC_THRESHOLD_GIB - 100
        ebs_throughput = Defaults.GP3_THROUGHPUT_MIB_PER_SEC
        instance_type = M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size)
        result = instance_type._calc_ebs_throughput()
        self.assertEqual(ebs_throughput, result)

    def test_calc_ebs_throughput__returns_max_gp2_throughput_above_threshold(self):
        ebs_size = Defaults.GP3_THROUGHPUT_CALC_THRESHOLD_GIB + 100
        ebs_throughput = Defaults.MAX_GP2_THROUGHPUT_MIB_PER_SEC
        instance_type = M6g.m6g_8xlarge().with_ebs_size_gb(ebs_size)
        result = instance_type._calc_ebs_throughput()
        self.assertEqual(ebs_throughput, result)
