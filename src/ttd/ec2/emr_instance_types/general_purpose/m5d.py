from ttd.ec2.emr_instance_type import EmrInstanceType


class M5d:

    # M5d - General purpose delta-accelarated machines
    @classmethod
    def m5d_xlarge(cls):
        return EmrInstanceType("m5d.xlarge", 4, 16)

    @classmethod
    def m5d_2xlarge(cls):
        return EmrInstanceType("m5d.2xlarge", 8, 32)

    @classmethod
    def m5d_4xlarge(cls):
        return EmrInstanceType("m5d.4xlarge", 16, 64)

    @classmethod
    def m5d_8xlarge(cls):
        return EmrInstanceType("m5d.8xlarge", 32, 128)

    @classmethod
    def m5d_12xlarge(cls):
        return EmrInstanceType("m5d.12xlarge", 48, 192)

    @classmethod
    def m5d_16xlarge(cls):
        return EmrInstanceType("m5d.16xlarge", 64, 256)

    @classmethod
    def m5d_24xlarge(cls):
        return EmrInstanceType("m5d.24xlarge", 96, 384)
