from ttd.ec2.emr_instance_type import EmrInstanceType


class M4:

    @classmethod
    def m4_large(cls):
        return EmrInstanceType("m4.large", 2, 8)

    @classmethod
    def m4_xlarge(cls):
        return EmrInstanceType("m4.xlarge", 4, 16)
