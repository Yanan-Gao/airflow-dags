from ttd.ec2.emr_instance_type import EmrInstanceType


class M5:

    @classmethod
    def m5_large(cls):
        return EmrInstanceType("m5.large", 2, 8)

    @classmethod
    def m5_xlarge(cls):
        return EmrInstanceType("m5.xlarge", 4, 16)

    @classmethod
    def m5_2xlarge(cls):
        return EmrInstanceType("m5.2xlarge", 8, 32)

    @classmethod
    def m5_4xlarge(cls):
        return EmrInstanceType("m5.4xlarge", 16, 64)

    @classmethod
    def m5_8xlarge(cls):
        return EmrInstanceType("m5.8xlarge", 32, 128)

    @classmethod
    def m5_12xlarge(cls):
        return EmrInstanceType("m5.12xlarge", 48, 192)

    @classmethod
    def m5_16xlarge(cls):
        return EmrInstanceType("m5.16xlarge", 64, 256)

    @classmethod
    def m5_24xlarge(cls):
        return EmrInstanceType("m5.24xlarge", 96, 384)
