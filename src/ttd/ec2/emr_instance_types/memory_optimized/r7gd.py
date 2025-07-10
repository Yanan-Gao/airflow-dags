from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.emr_version import EmrVersion


class R7gd:
    """
    Amazon EC2 R7gd Instances
    =========================

    Amazon Elastic Compute Cloud (EC2) R7g instances, powered by the latest generation AWS Graviton3 processors,
    provide high price performance in Amazon EC2 for memory-intensive workloads.
    R7g instances are ideal for memory-intensive workloads such as open-source databases, in-memory caches,
    and real-time big data analytics.
    They offer up to 25% better performance over the sixth-generation AWS Graviton2-based R6g instances.
    R7g instances feature Double Data Rate 5 (DDR5) memory, which provides 50% higher memory bandwidth compared
    to DDR4 memory to enable high-speed access to data in memory.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    Open-source databases, in-memory caches, and real time big data analytics.
    """

    minimum_emr_versions = [
        EmrVersion("emr-5.36.1"),
        EmrVersion("emr-6.10.0"),
        EmrVersion("emr-7.0.0"),
    ]

    ## not supported by EMR
    # @classmethod
    # def r7gd_large(cls):
    #     return EmrInstanceType("r7gd.large", 2, 16)

    @classmethod
    def r7gd_xlarge(cls):
        return EmrInstanceType("r7gd.xlarge", 4, 32, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7gd_2xlarge(cls):
        return EmrInstanceType("r7gd.2xlarge", 8, 64, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7gd_4xlarge(cls):
        return EmrInstanceType("r7gd.4xlarge", 16, 128, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7gd_8xlarge(cls):
        return EmrInstanceType("r7gd.8xlarge", 32, 256, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7gd_12xlarge(cls):
        return EmrInstanceType("r7gd.12xlarge", 48, 384, minimum_emr_versions=cls.minimum_emr_versions)

    @classmethod
    def r7gd_16xlarge(cls):
        return EmrInstanceType("r7gd.16xlarge", 64, 512, minimum_emr_versions=cls.minimum_emr_versions)
