from ttd.ec2.emr_instance_type import EmrInstanceType


class R6i:
    """
    Amazon EC2 R6i Instances
    ========================

    Amazon EC2 R6i Instances are powered by 3rd generation Intel Xeon Scalable processors and deliver up to 15% better price
    performance compared to R5 instances.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    These instances are SAP-Certified and are an ideal fit for memory-intensive workloads, such as SQL and NoSQL databases; distributed
    web scale in-memory caches, such as Memcached and Redis; in-memory databases, such as SAP HANA; and real-time big data analytics,
    such as Apache Hadoop and Apache Spark clusters.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Up to 3.5 GHz 3rd generation Intel Xeon Scalable processors
    - Up to 128 vCPUs and up to 1,024 GiB of memory
    - Up to 50 Gbps network bandwidth and up to 40 Gbps EBS bandwidth

    `Docs <https://aws.amazon.com/ec2/instance-types/r6i/>`_
    """

    ## not supported by EMR
    # @classmethod
    # def r6i_large(cls):
    #     return EmrInstanceType("r6i.large", 2, 16)

    @classmethod
    def r6i_xlarge(cls):
        return EmrInstanceType("r6i.xlarge", 4, 32)

    @classmethod
    def r6i_2xlarge(cls):
        return EmrInstanceType("r6i.2xlarge", 8, 64)

    @classmethod
    def r6i_4xlarge(cls):
        return EmrInstanceType("r6i.4xlarge", 16, 128)

    @classmethod
    def r6i_8xlarge(cls):
        return EmrInstanceType("r6i.8xlarge", 32, 256)

    @classmethod
    def r6i_12xlarge(cls):
        return EmrInstanceType("r6i.12xlarge", 48, 384)

    @classmethod
    def r6i_16xlarge(cls):
        return EmrInstanceType("r6i.16xlarge", 64, 512)

    @classmethod
    def r6i_24xlarge(cls):
        return EmrInstanceType("r6i.24xlarge", 96, 768)

    @classmethod
    def r6i_32xlarge(cls):
        return EmrInstanceType("r6i.32xlarge", 128, 1024)
