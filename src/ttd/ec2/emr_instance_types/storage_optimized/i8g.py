from ttd.ec2.emr_instance_type import EmrInstanceType


class I8g:

    @classmethod
    def i8g_large(cls):
        return EmrInstanceType("i8g.large", 2, 16)

    @classmethod
    def i8g_xlarge(cls):
        return EmrInstanceType("i8g.xlarge", 4, 32)

    @classmethod
    def i8g_2xlarge(cls):
        return EmrInstanceType("i8g.2xlarge", 8, 64)

    @classmethod
    def i8g_4xlarge(cls):
        return EmrInstanceType("i8g.4xlarge", 16, 128)

    @classmethod
    def i8g_8xlarge(cls):
        return EmrInstanceType("i8g.8xlarge", 32, 256)

    @classmethod
    def i8g_16xlarge(cls):
        return EmrInstanceType("i8g.16xlarge", 64, 512)

    @classmethod
    def i8g_24xlarge(cls):
        return EmrInstanceType("i8g.24xlarge", 96, 768)

    @classmethod
    def i8g_48xlarge(cls):
        return EmrInstanceType("i8g.48xlarge", 192, 1536)
