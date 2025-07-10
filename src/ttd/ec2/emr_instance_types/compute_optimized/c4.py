from ttd.ec2.emr_instance_type import EmrInstanceType


class C4:

    @classmethod
    def c4_large(cls):
        return EmrInstanceType("c4.large", 2, 3)
