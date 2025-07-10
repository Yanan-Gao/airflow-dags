from ttd.ec2.emr_instance_type import EmrInstanceType


class R5d:
    """
    Amazon EC2 R5d Instances
    ========================

    Amazon EC2 R5d Instances are Memory Optimized instances that accelerate
    performance for workloads that process large data sets in memory including application that can benefit from local NVMe-based SSDs
    that are physically connected to the host server.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    High performance databases, distributed web scale in-memory caches, mid-size in-memory databases, real time big data analytics

    Key technology:
    ^^^^^^^^^^^^^^^
    - 3.1 GHz Intel Xeon Platinum Processor
    - Up to 96 vCPUs
    - Up to 768 GiB of Memory
    - Up to 25 Gbps network bandwidth
    - Up to 3.6 TB NVMe SSD instance storage

    """

    ## not supported by EMR
    # @classmethod
    # def r5d_large(cls):
    #     return EmrInstanceType("r5d.large", 2, 16)

    @classmethod
    def r5d_xlarge(cls):
        return EmrInstanceType("r5d.xlarge", 4, 32)

    @classmethod
    def r5d_2xlarge(cls):
        return EmrInstanceType("r5d.2xlarge", 8, 64)

    @classmethod
    def r5d_4xlarge(cls):
        return EmrInstanceType("r5d.4xlarge", 16, 128)

    @classmethod
    def r5d_8xlarge(cls):
        return EmrInstanceType("r5d.8xlarge", 32, 256)

    @classmethod
    def r5d_12xlarge(cls):
        return EmrInstanceType("r5d.12xlarge", 48, 384)

    @classmethod
    def r5d_16xlarge(cls):
        return EmrInstanceType("r5d.16xlarge", 64, 512)

    @classmethod
    def r5d_24xlarge(cls):
        return EmrInstanceType("r5d.24xlarge", 96, 768)
