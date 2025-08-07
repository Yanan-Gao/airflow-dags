import unittest

from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import EbsAwsInstanceStorage
from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.emr_version import EmrVersion


class TestEmrInstanceClass(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @staticmethod
    def get_instance_a() -> EmrInstanceType:
        instance = EmrInstanceType(
            "x8g.xlarge",
            4,
            30.5,
            32,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True),
            [
                EmrVersion("emr-7.0.0"),
            ],
        )
        return instance

    @staticmethod
    def get_instance_b() -> EmrInstanceType:
        instance = EmrInstanceType(
            "x7g.xlarge",
            4,
            30.5,
            32,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True),
            [
                EmrVersion("emr-7.0.0"),
            ],
        )
        return instance

    def test__weighted_capacity__is_adjustable(self):
        instance = self.get_instance_a()
        cls = EmrInstanceClass([instance])

        before_weighted_capacity = cls.get_instance_types()[0].weighted_capacity
        self.assertEqual(before_weighted_capacity, instance.cores)

        expected_weighted_capacity = 1
        cls = cls.with_weighted_capacity(expected_weighted_capacity)
        after_instance = cls.get_instance_types()[0]
        after_weighted_capacity = after_instance.weighted_capacity
        self.assertEqual(after_weighted_capacity, expected_weighted_capacity)

        after_instance.as_instance_spec()

    def test__priority__is_adjustable(self):
        cls = EmrInstanceClass([self.get_instance_a()], [self.get_instance_b()])

        priority = cls.with_fallback_instance().get_instance_types()[1].priority
        self.assertEqual(priority, 2)
