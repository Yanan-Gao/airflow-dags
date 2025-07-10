from ttd.ec2.emr_instance_type import EmrInstanceType


class C7g:
    """
    ========================
    Amazon EC2 C7g Instances
    ========================

    Amazon EC2 C7g instances, powered by the latest generation AWS Graviton3 processors, provide the best price performance in Amazon
    EC2 for compute-intensive workloads. They offer up to 25% better performance over the sixth generation AWS Graviton2-based C6g
    instances.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    These instances are ideal for high performance computing (HPC), batch processing, electronic design automation (EDA), gaming,
    video encoding, scientific modeling, distributed analytics, CPU-based machine learning (ML) inference, and ad-serving.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Custom built AWS Graviton3 processor with 64-bit Arm Neoverse-V1 cores
    - Up to 64 vCPUs and up to 128 GiB of memory
    - Up to 30 Gbps network bandwidth and up to 20 Gbps EBS bandwidth
    """

    ## not supported for EMR
    # @classmethod
    # def c7g_large(cls):
    #     return EmrInstanceType("c7g.large", 2, 4)

    @classmethod
    def c7g_xlarge(cls):
        return EmrInstanceType("c7g.xlarge", 4, 7.5, 8)

    @classmethod
    def c7g_2xlarge(cls):
        return EmrInstanceType("c7g.2xlarge", 8, 15.25, 16)

    @classmethod
    def c7g_4xlarge(cls):
        return EmrInstanceType("c7g.4xlarge", 16, 30.5, 32)

    @classmethod
    def c7g_8xlarge(cls):
        return EmrInstanceType("c7g.8xlarge", 32, 61, 64)

    @classmethod
    def c7g_12xlarge(cls):
        return EmrInstanceType("c7g.12xlarge", 48, 91.5, 96)

    @classmethod
    def c7g_16xlarge(cls):
        return EmrInstanceType("c7g.16xlarge", 64, 122, 128)
