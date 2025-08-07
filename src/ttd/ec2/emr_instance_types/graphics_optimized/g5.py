from ttd.ec2.emr_instance_type import EmrInstanceType


class G5:

    @classmethod
    def g5_xlarge(cls):
        return EmrInstanceType("g5.xlarge", 4, 16)

    @classmethod
    def g5_2xlarge(cls):
        return EmrInstanceType("g5.2xlarge", 8, 32)

    @classmethod
    def g5_4xlarge(cls):
        return EmrInstanceType("g5.4xlarge", 16, 61)

    @classmethod
    def g5_8xlarge(cls):
        return EmrInstanceType("g5.8xlarge", 32, 122)

    @classmethod
    def g5_16xlarge(cls):
        return EmrInstanceType("g5.16xlarge", 64, 244)

    @classmethod
    def g5_24xlarge(cls):
        return EmrInstanceType("g5.24xlarge", 96, 366)

    @classmethod
    def g5_48xlarge(cls):
        return EmrInstanceType("g5.48xlarge", 192, 732)
