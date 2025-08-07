from ttd.ec2.emr_instance_type import EmrInstanceType


class I4g:

    @classmethod
    def i4g_large(cls):
        return EmrInstanceType("i4g.large", 2, 16)

    @classmethod
    def i4g_xlarge(cls):
        return EmrInstanceType("i4g.xlarge", 4, 32)

    @classmethod
    def i4g_2xlarge(cls):
        return EmrInstanceType("i4g.2xlarge", 8, 64)

    @classmethod
    def i4g_4xlarge(cls):
        return EmrInstanceType("i4g.4xlarge", 16, 128)

    @classmethod
    def i4g_8xlarge(cls):
        return EmrInstanceType("i4g.8xlarge", 32, 256)

    @classmethod
    def i4g_16xlarge(cls):
        return EmrInstanceType("i4g.16xlarge", 64, 512)
