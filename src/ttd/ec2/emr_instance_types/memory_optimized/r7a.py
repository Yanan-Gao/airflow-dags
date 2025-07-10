from ttd.ec2.emr_instance_type import EmrInstanceType


class R7a:
    """
    Amazon EC2 R7a Instances
    ========================

    Amazon EC2 R7a instances, powered by 4th generation AMD EPYC processors,
    deliver up to 50% higher performance compared to R6a instances..

    Ideal Use cases: R7a instances are SAP-certified and ideal for high performance, memory-intensive workloads,
    such as SQL and NoSQL databases, distributed web scale in-memory caches, in-memory databases, real-time big data analytics,
    and Electronic Design Automation (EDA) applications.

    """

    @classmethod
    def r7a_large(cls):
        return EmrInstanceType("r7a.large", 2, 16)

    @classmethod
    def r7a_xlarge(cls):
        return EmrInstanceType("r7a.xlarge", 4, 32)

    @classmethod
    def r7a_2xlarge(cls):
        return EmrInstanceType("r7a.2xlarge", 8, 64)

    @classmethod
    def r7a_4xlarge(cls):
        return EmrInstanceType("r7a.4xlarge", 16, 128)

    @classmethod
    def r7a_8xlarge(cls):
        return EmrInstanceType("r7a.8xlarge", 32, 256)

    @classmethod
    def r7a_12xlarge(cls):
        return EmrInstanceType("r7a.12xlarge", 48, 384)

    @classmethod
    def r7a_16xlarge(cls):
        return EmrInstanceType("r7a.16xlarge", 64, 512)

    @classmethod
    def r7a_24xlarge(cls):
        return EmrInstanceType("r7a.24xlarge", 96, 768)

    @classmethod
    def r7a_32xlarge(cls):
        return EmrInstanceType("r7a.32xlarge", 128, 1024)

    @classmethod
    def r7a_48xlarge(cls):
        return EmrInstanceType("r7a.48xlarge", 192, 1536)
