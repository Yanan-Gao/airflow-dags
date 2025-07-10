from ttd.ec2.emr_instance_type import EmrInstanceType


class R5a:
    """
    Amazon EC2 R5a Instances
    ========================

    Amazon EC2 R5a Instances are Memory Optimized instances that accelerate performance for workloads that process large data sets in
    memory. R5a instances deliver up to 10% cost savings over comparable instance types.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    High performance databases, distributed web scale in-memory caches, mid-size in-memory databases, real time big data analytics

    Key technology:
    ^^^^^^^^^^^^^^^
    - 2.5 GHz AMD EPYC 7000 series processors
    - Up to 96 vCPUs
    - Up to 768 GiB of Memory
    - Up to 20 Gbps network bandwidth


    """

    @classmethod
    def r5a_xlarge(cls):
        return EmrInstanceType("r5a.xlarge", 4, 32)

    @classmethod
    def r5a_2xlarge(cls):
        return EmrInstanceType("r5a.2xlarge", 8, 64)

    @classmethod
    def r5a_4xlarge(cls):
        return EmrInstanceType("r5a.4xlarge", 16, 128)

    @classmethod
    def r5a_8xlarge(cls):
        return EmrInstanceType("r5a.8xlarge", 32, 256)

    @classmethod
    def r5a_12xlarge(cls):
        return EmrInstanceType("r5a.12xlarge", 48, 384)

    @classmethod
    def r5a_16xlarge(cls):
        return EmrInstanceType("r5a.16xlarge", 64, 512)

    @classmethod
    def r5a_24xlarge(cls):
        return EmrInstanceType("r5a.24xlarge", 96, 768)
