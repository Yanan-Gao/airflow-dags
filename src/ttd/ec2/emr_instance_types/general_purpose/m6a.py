from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import EbsAwsInstanceStorage
from ttd.ec2.emr_instance_type import EmrInstanceType


class M6a:

    # m6a

    @classmethod
    def m6a_large(cls):
        return EmrInstanceType(
            "m6a.large", 2, 8, 8, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m6a_xlarge(cls):
        return EmrInstanceType(
            "m6a.xlarge", 4, 15.25, 16, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m6a_2xlarge(cls):
        return EmrInstanceType(
            "m6a.2xlarge", 8, 30.5, 32, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m6a_4xlarge(cls):
        return EmrInstanceType(
            "m6a.4xlarge", 16, 61, 64, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m6a_8xlarge(cls):
        return EmrInstanceType("m6a.8xlarge", 32, 122, 128, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5), AwsInstanceBandwidth(10))

    @classmethod
    def m6a_12xlarge(cls):
        return EmrInstanceType("m6a.12xlarge", 48, 183, 192, EbsAwsInstanceStorage(), AwsInstanceBandwidth(18.75), AwsInstanceBandwidth(15))

    @classmethod
    def m6a_16xlarge(cls):
        return EmrInstanceType("m6a.16xlarge", 64, 244, 256, EbsAwsInstanceStorage(), AwsInstanceBandwidth(25), AwsInstanceBandwidth(20))
