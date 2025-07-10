from ttd.ec2.emr_instance_type import EmrInstanceType


class G4:

    @classmethod
    def g4dn_xlarge(cls):
        return EmrInstanceType("g4dn.xlarge", 4, 16)

    @classmethod
    def g4dn_4xlarge(cls):
        return EmrInstanceType("g4dn.4xlarge", 16, 64)

    @classmethod
    def g4dn_8xlarge(cls):
        return EmrInstanceType("g4dn.8xlarge", 32, 128)

    @classmethod
    def g4dn_12xlarge(cls):
        return EmrInstanceType("g4dn.12xlarge", 48, 192)

    @classmethod
    def g4dn_16xlarge(cls):
        return EmrInstanceType("g4dn.16xlarge", 64, 256)
