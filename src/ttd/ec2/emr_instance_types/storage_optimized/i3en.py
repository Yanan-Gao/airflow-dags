from ttd.ec2.emr_instance_type import EmrInstanceType


class I3en:
    # i3en

    @classmethod
    def i3en_large(cls):
        return EmrInstanceType("i3en.large", 2, 16)

    @classmethod
    def i3en_xlarge(cls):
        return EmrInstanceType("i3en.xlarge", 4, 32)

    @classmethod
    def i3en_2xlarge(cls):
        return EmrInstanceType("i3en.2xlarge", 8, 64)

    @classmethod
    def i3en_3xlarge(cls):
        return EmrInstanceType("i3en.3xlarge", 12, 96)

    @classmethod
    def i3en_6xlarge(cls):
        return EmrInstanceType("i3en.6xlarge", 24, 192)

    @classmethod
    def i3en_12xlarge(cls):
        return EmrInstanceType("i3en.12xlarge", 48, 384)

    @classmethod
    def i3en_24xlarge(cls):
        return EmrInstanceType("i3en.24xlarge", 96, 768)
