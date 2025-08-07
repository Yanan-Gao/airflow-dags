from ttd.ec2.emr_instance_type import EmrInstanceType


class I3:
    # i class
    @classmethod
    def i3_2xlarge(cls):
        return EmrInstanceType("i3.2xlarge", 8, 61)

    @classmethod
    def i3_4xlarge(cls):
        return EmrInstanceType("i3.4xlarge", 16, 122)

    @classmethod
    def i3_8xlarge(cls):
        return EmrInstanceType("i3.8xlarge", 32, 244)

    @classmethod
    def i3_16xlarge(cls):
        return EmrInstanceType("i3.16xlarge", 64, 448)
