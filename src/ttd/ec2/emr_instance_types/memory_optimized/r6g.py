from ttd.ec2.emr_instance_type import EmrInstanceType


class R6g:
    """
    Amazon EC2 R6g Instances
    =========================

    Amazon EC2 R6g instances are memory-optimized instances powered by Arm-based AWS Graviton2 processors. They deliver up to 40%
    better price performance over current generation R5 instances for memory-intensive workloads.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    Open-source databases, in-memory caches, and real time big data analytics.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Custom built AWS Graviton2 Processor with 64-bit Arm Neoverse cores
    - Up to 64 vCPUs
    - Up to 512 GiB of Memory
    - Up to 25 Gbps network bandwidth and up to 19 Gbps EBS bandwidth


    """

    ## not supported by EMR
    # @classmethod
    # def r6g_large(cls):
    #     return EmrInstanceType("r6g.large", 2, 16)

    @classmethod
    def r6g_xlarge(cls):
        return EmrInstanceType("r6g.xlarge", 4, 32)

    @classmethod
    def r6g_2xlarge(cls):
        return EmrInstanceType("r6g.2xlarge", 8, 64)

    @classmethod
    def r6g_4xlarge(cls):
        return EmrInstanceType("r6g.4xlarge", 16, 128)

    @classmethod
    def r6g_8xlarge(cls):
        return EmrInstanceType("r6g.8xlarge", 32, 256)

    @classmethod
    def r6g_12xlarge(cls):
        return EmrInstanceType("r6g.12xlarge", 48, 384)

    @classmethod
    def r6g_16xlarge(cls):
        return EmrInstanceType("r6g.16xlarge", 64, 512)
