from ttd.ec2.emr_instance_type import EmrInstanceType


class I4i:

    @classmethod
    def i4i_large(cls):
        return EmrInstanceType("i4i.large", 2, 16)

    @classmethod
    def i4i_xlarge(cls):
        return EmrInstanceType("i4i.xlarge", 4, 32)

    @classmethod
    def i4i_2xlarge(cls):
        return EmrInstanceType("i4i.2xlarge", 8, 64)

    @classmethod
    def i4i_4xlarge(cls):
        return EmrInstanceType("i4i.4xlarge", 16, 128)

    @classmethod
    def i4i_8xlarge(cls):
        return EmrInstanceType("i4i.8xlarge", 32, 256)

    @classmethod
    def i4i_16xlarge(cls):
        return EmrInstanceType("i4i.16xlarge", 64, 512)

    @classmethod
    def i4i_24xlarge(cls):
        return EmrInstanceType("i4i.24xlarge", 96, 768)

    @classmethod
    def i4i_32xlarge(cls):
        return EmrInstanceType("i4i.32xlarge", 128, 1024)
