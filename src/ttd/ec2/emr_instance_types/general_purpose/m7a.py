from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import EbsAwsInstanceStorage
from ttd.ec2.emr_instance_type import EmrInstanceType


class M7a:

    # m7a
    @classmethod
    def m7a_medium(cls):
        return EmrInstanceType(
            "m7a.medium", 1, 4, 4, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7a_large(cls):
        return EmrInstanceType(
            "m7a.large", 2, 8, 8, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7a_xlarge(cls):
        return EmrInstanceType(
            "m7a.xlarge", 4, 15.25, 16, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7a_2xlarge(cls):
        return EmrInstanceType(
            "m7a.2xlarge", 8, 30.5, 32, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7a_4xlarge(cls):
        return EmrInstanceType(
            "m7a.4xlarge", 16, 61, 64, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7a_8xlarge(cls):
        return EmrInstanceType("m7a.8xlarge", 32, 122, 128, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5), AwsInstanceBandwidth(10))

    @classmethod
    def m7a_12xlarge(cls):
        return EmrInstanceType("m7a.12xlarge", 48, 183, 192, EbsAwsInstanceStorage(), AwsInstanceBandwidth(18.75), AwsInstanceBandwidth(15))

    @classmethod
    def m7a_16xlarge(cls):
        return EmrInstanceType("m7a.16xlarge", 64, 244, 256, EbsAwsInstanceStorage(), AwsInstanceBandwidth(25), AwsInstanceBandwidth(20))
