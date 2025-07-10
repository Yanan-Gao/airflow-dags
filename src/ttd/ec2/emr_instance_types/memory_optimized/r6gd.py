from ttd.ec2.emr_instance_type import EmrInstanceType


class R6gd:
    """
    Amazon EC2 R6gd Instances
    =========================

    Amazon EC2 R6gd instances are memory-optimized instances powered by Arm-based AWS Graviton2 processors and include local NVMe-based
    SSDs that are physically connected to the host server. They deliver up to 40% better price performance over current generation R5
    instances for memory-intensive workloads.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    Open-source databases, in-memory caches, and real time big data analytics.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Custom built AWS Graviton2 Processor with 64-bit Arm Neoverse cores
    - Up to 64 vCPUs, Up to 512 GiB of Memory
    - Up to 25 Gbps network bandwidth and up to 19 Gbps EBS bandwidth
    - Up to 3.8 TB NVMe SSD instance storage


    """

    @classmethod
    def r6gd_large(cls):
        return EmrInstanceType("r6gd.large", 2, 16)

    @classmethod
    def r6gd_xlarge(cls):
        return EmrInstanceType("r6gd.xlarge", 4, 32)

    @classmethod
    def r6gd_2xlarge(cls):
        return EmrInstanceType("r6gd.2xlarge", 8, 64)

    @classmethod
    def r6gd_4xlarge(cls):
        return EmrInstanceType("r6gd.4xlarge", 16, 128)

    @classmethod
    def r6gd_8xlarge(cls):
        return EmrInstanceType("r6gd.8xlarge", 32, 256)

    @classmethod
    def r6gd_12xlarge(cls):
        return EmrInstanceType("r6gd.12xlarge", 48, 384)

    @classmethod
    def r6gd_16xlarge(cls):
        return EmrInstanceType("r6gd.16xlarge", 64, 512)
