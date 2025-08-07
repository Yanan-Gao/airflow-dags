from unittest import TestCase
from unittest.mock import patch

from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.operators.ec2_subnets_operator import EC2SubnetsOperator

instance_offerings_list = [
    (
        [M5.m5_2xlarge(), C5.c5_xlarge()],
        [
            {
                "InstanceType": "m5.2xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1a",
            },
            {
                "InstanceType": "m5.2xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1b",
            },
            {
                "InstanceType": "c5.xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1a",
            },
            {
                "InstanceType": "c5.xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1b",
            },
        ],
        {"us-east-1a", "us-east-1b"},
    ),
    (
        [M5.m5_2xlarge(), C5.c5_xlarge()],
        [
            {
                "InstanceType": "m5.2xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1a",
            },
            {
                "InstanceType": "m5.2xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1b",
            },
            {
                "InstanceType": "c5.xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1a",
            },
        ],
        {"us-east-1a"},
    ),
    (
        [M5.m5_2xlarge(), C5.c5_xlarge()],
        [
            {
                "InstanceType": "m5.2xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1a",
            },
            {
                "InstanceType": "c5.xlarge",
                "LocationType": "availability-zone",
                "Location": "us-east-1b",
            },
        ],
        set(),
    ),
    ([], [], set()),
]


class TestEC2SubnetsOperator(TestCase):

    @patch("ttd.operators.ec2_subnets_operator.EC2Hook")
    def test_get_common_zones(self, mock_ec2hook):
        for instance_types, offerings, common_zones_expected in instance_offerings_list:
            with (self.subTest()):
                mock_ec2hook.return_value.conn.describe_instance_type_offerings.return_value = {"InstanceTypeOfferings": offerings}
                operator = EC2SubnetsOperator(name="test-name", instance_types=instance_types)
                common_zones_result = operator.get_common_zones()
                self.assertEqual(common_zones_result, common_zones_expected)
