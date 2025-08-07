from ttd.ec2.emr_instance_type import EmrInstanceType


class R5b:
    """
    Amazon EC2 R5b Instances
    ========================

    Amazon EC2 R5b instances are EBS-optimized variants of memory-optimized R5 instances. R5b instances increase EBS performance by 3x
    compared to same-sized R5 instances. R5b instances deliver up to 60 Gbps bandwidth and 260K IOPS of EBS performance,
    the fastest block storage performance on EC2.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    High performance databases, distributed web scale in-memory caches, mid-size in-memory databases, real time big data analytics.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Custom 2nd generation Intel Xeon Scalable Processors (Cascade Lake) with a sustained all-core Turbo CPU frequency of 3.1 GHz and
    maximum single core turbo frequency of 3.5 GHz
    - Up to 96 vCPUs
    - Up to 768 GiB of Memory
    - Up to 25 Gbps network bandwidth
    - Up to 60 Gbps of EBS bandwidth
    """

    @classmethod
    def r5b_xlarge(cls):
        return EmrInstanceType("r5b.xlarge", 4, 32)

    @classmethod
    def r5b_2xlarge(cls):
        return EmrInstanceType("r5b.2xlarge", 8, 64)

    @classmethod
    def r5b_4xlarge(cls):
        return EmrInstanceType("r5b.4xlarge", 16, 128)

    @classmethod
    def r5b_8xlarge(cls):
        return EmrInstanceType("r5b.8xlarge", 32, 256)

    @classmethod
    def r5b_12xlarge(cls):
        return EmrInstanceType("r5b.12xlarge", 48, 384)

    @classmethod
    def r5b_16xlarge(cls):
        return EmrInstanceType("r5b.16xlarge", 64, 512)

    @classmethod
    def r5b_24xlarge(cls):
        return EmrInstanceType("r5b.24xlarge", 96, 768)
