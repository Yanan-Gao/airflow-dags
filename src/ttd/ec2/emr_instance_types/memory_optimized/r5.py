from ttd.ec2.emr_instance_type import EmrInstanceType


class R5:
    """
    Amazon EC2 R5 Instances
    =======================

    Amazon EC2 R5 Instances are Memory Optimized instances that accelerate performance for workloads that process large data sets in
    memory.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    High performance databases, distributed web scale in-memory caches, mid-size in-memory databases, real time big data analytics

    Key technology:
    ^^^^^^^^^^^^^^^
    - 3.1 GHz Intel Xeon Platinum Processor
    - Up to 96 vCPUs
    - Up to 768 GiB of Memory
    - Up to 25 Gbps network bandwidth
    """

    @classmethod
    def r5_large(cls):
        return EmrInstanceType("r5.large", 2, 16).with_cluster_calc_unsupported()

    @classmethod
    def r5_xlarge(cls):
        return EmrInstanceType("r5.xlarge", 4, 32)

    @classmethod
    def r5_2xlarge(cls):
        return EmrInstanceType("r5.2xlarge", 8, 64)

    @classmethod
    def r5_4xlarge(cls):
        return EmrInstanceType("r5.4xlarge", 16, 128)

    @classmethod
    def r5_8xlarge(cls):
        return EmrInstanceType("r5.8xlarge", 32, 256)

    @classmethod
    def r5_12xlarge(cls):
        return EmrInstanceType("r5.12xlarge", 48, 384)

    @classmethod
    def r5_16xlarge(cls):
        return EmrInstanceType("r5.16xlarge", 64, 512)

    @classmethod
    def r5_24xlarge(cls):
        return EmrInstanceType("r5.24xlarge", 96, 768)
