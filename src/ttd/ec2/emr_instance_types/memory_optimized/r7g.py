from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.emr_version import EmrVersion


class R7g:
    """
    Amazon EC2 r7g Instances
    =========================

    Amazon EC2 R7g instances  are powered by AWS Graviton3 processors. They deliver high price performance in Amazon EC2 for memory-optimized applications.

    Features:
    Powered by custom-built AWS Graviton3 processors
    Features DDR5 memory that offers 50% more bandwidth compared to DDR4
    20% higher enhanced networking bandwidth compared to r6g instances
    EBS-optimized by default
    Instance storage offered via EBS
    Supports Elastic Fabric Adapter (EFA) on r7g.16xlarge, r7g.metal instances
    Powered by the AWS Nitro System, a combination of dedicated hardware and lightweight hypervisor
    """

    minimum_emr_versions = [
        EmrVersion("emr-5.36.1"),
        EmrVersion("emr-6.10.0"),
        EmrVersion("emr-7.0.0"),
    ]

    ## not supported by EMR
    # @classmethod
    # def r7g_large(cls):
    #     return EmrInstanceType("r7g.large", 2, 16)

    @classmethod
    def r7g_xlarge(cls):
        return EmrInstanceType("r7g.xlarge", 4, 30.5, declared_memory=32, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7g_2xlarge(cls):
        return EmrInstanceType("r7g.2xlarge", 8, 61, declared_memory=65, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7g_4xlarge(cls):
        return EmrInstanceType("r7g.4xlarge", 16, 122, declared_memory=128, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7g_8xlarge(cls):
        return EmrInstanceType("r7g.8xlarge", 32, 244, declared_memory=256, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7g_12xlarge(cls):
        return EmrInstanceType("r7g.12xlarge", 48, 366, declared_memory=384, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7g_16xlarge(cls):
        return EmrInstanceType("r7g.16xlarge", 64, memory=488, declared_memory=512, minimum_emr_versions=cls.minimum_emr_versions)
