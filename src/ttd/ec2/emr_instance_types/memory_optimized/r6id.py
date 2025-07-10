from ttd.ec2.emr_instance_type import EmrInstanceType


class R6id:
    """
    Amazon EC2 R6id Instances
    =========================

    Amazon EC2 R6id instances are powered by 3rd generation Intel Xeon Scalable processors and deliver up to 15% better price
    performance compared to R5d instances. R6id instances come with local NVMe-based SSD block-level storage for applications that need
    high-speed, low-latency local storage.

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
    - Up to 7.6 TB NVMe SSD instance storage

    `Docs <https://aws.amazon.com/ec2/instance-types/r6i/>`_
    """

    @classmethod
    def r6id_large(cls):
        return EmrInstanceType("r6id.large", 2, 16)

    @classmethod
    def r6id_xlarge(cls):
        return EmrInstanceType("r6id.xlarge", 4, 32)

    @classmethod
    def r6id_2xlarge(cls):
        return EmrInstanceType("r6id.2xlarge", 8, 64)

    @classmethod
    def r6id_4xlarge(cls):
        return EmrInstanceType("r6id.4xlarge", 16, 128)

    @classmethod
    def r6id_8xlarge(cls):
        return EmrInstanceType("r6id.8xlarge", 32, 256)

    @classmethod
    def r6id_12xlarge(cls):
        return EmrInstanceType("r6id.12xlarge", 48, 384)

    @classmethod
    def r6id_16xlarge(cls):
        return EmrInstanceType("r6id.16xlarge", 64, 512)

    @classmethod
    def r6id_24xlarge(cls):
        return EmrInstanceType("r6id.24xlarge", 96, 768)

    @classmethod
    def r6id_32xlarge(cls):
        return EmrInstanceType("r6id.32xlarge", 128, 1024)
