from ttd.ec2.emr_instance_type import EmrInstanceType


class R7i:
    """
    Amazon EC2 R7i Instances
    ========================

    Amazon Elastic Compute Cloud (Amazon EC2) R7i instances are next-generation memory optimized instances powered by custom
    4th Generation Intel Xeon Scalable processors (code named Sapphire Rapids) and feature an 8:1 ratio of memory to vCPU.
    EC2 instances powered by these custom processors, available only on AWS, offer the best performance among comparable Intel
    processors in the cloud – up to 15% better performance than Intel processors utilized by other cloud providers.

    R7i instances deliver up to 15% better price performance compared to R6i instances. R7i instances are SAP-certified and ideal
    for all memory-intensive workloads (SQL and NoSQL databases), distributed web scale in-memory caches (Memcached and Redis),
    in-memory databases (SAP HANA), and real-time big data analytics (Apache Hadoop and Apache Spark clusters).

    """

    ## not supported by EMR
    # @classmethod
    # def r7i_large(cls):
    #     return EmrInstanceType("r7i.large", 2, 16)

    @classmethod
    def r7i_xlarge(cls):
        return EmrInstanceType("r7i.xlarge", 4, 32)

    @classmethod
    def r7i_2xlarge(cls):
        return EmrInstanceType("r7i.2xlarge", 8, 64)

    @classmethod
    def r7i_4xlarge(cls):
        return EmrInstanceType("r7i.4xlarge", 16, 128)

    @classmethod
    def r7i_8xlarge(cls):
        return EmrInstanceType("r7i.8xlarge", 32, 256)

    @classmethod
    def r7i_12xlarge(cls):
        return EmrInstanceType("r7i.12xlarge", 48, 384)

    @classmethod
    def r7i_16xlarge(cls):
        return EmrInstanceType("r7i.16xlarge", 64, 512)

    @classmethod
    def r7i_24xlarge(cls):
        return EmrInstanceType("r7i.24xlarge", 96, 768)

    @classmethod
    def r7i_48xlarge(cls):
        return EmrInstanceType("r7i.48xlarge", 192, 1536)
