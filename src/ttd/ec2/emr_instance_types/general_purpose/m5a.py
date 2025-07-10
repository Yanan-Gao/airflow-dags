from ttd.ec2.emr_instance_type import EmrInstanceType


class M5a:

    # M5a - General purpose AMD-based machines
    @classmethod
    def m5a_large(cls):
        return EmrInstanceType("m5a.large", 2, 8)

    @classmethod
    def m5a_xlarge(cls):
        return EmrInstanceType("m5a.xlarge", 4, 16)

    @classmethod
    def m5a_2xlarge(cls):
        return EmrInstanceType("m5a.2xlarge", 8, 32)

    @classmethod
    def m5a_4xlarge(cls):
        return EmrInstanceType("m5a.4xlarge", 16, 64)

    @classmethod
    def m5a_8xlarge(cls):
        return EmrInstanceType("m5a.8xlarge", 32, 128)

    @classmethod
    def m5a_12xlarge(cls):
        return EmrInstanceType("m5a.12xlarge", 48, 192)

    @classmethod
    def m5a_16xlarge(cls):
        return EmrInstanceType("m5a.16xlarge", 64, 256)

    @classmethod
    def m5a_24xlarge(cls):
        return EmrInstanceType("m5a.24xlarge", 96, 384)
