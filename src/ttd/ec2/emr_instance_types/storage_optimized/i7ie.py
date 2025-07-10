from ttd.ec2.emr_instance_type import EmrInstanceType


class I7ie:

    @classmethod
    def i7ie_large(cls):
        return EmrInstanceType("i7ie.large", 2, 16)

    @classmethod
    def i7ie_xlarge(cls):
        return EmrInstanceType("i7ie.xlarge", 4, 32)

    @classmethod
    def i7ie_2xlarge(cls):
        return EmrInstanceType("i7ie.2xlarge", 8, 64)

    @classmethod
    def i7ie_12xlarge(cls):
        return EmrInstanceType("i7ie.12xlarge", 48, 384)

    @classmethod
    def i7ie_18xlarge(cls):
        return EmrInstanceType("i7ie.18xlarge", 72, 576)

    @classmethod
    def i7ie_24xlarge(cls):
        return EmrInstanceType("i7ie.24xlarge", 96, 768)
