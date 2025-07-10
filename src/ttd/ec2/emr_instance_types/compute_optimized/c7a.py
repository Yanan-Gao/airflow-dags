from ttd.ec2.emr_instance_type import EmrInstanceType


class C7a:
    """
    ========================
    Amazon EC2 c7a Instances
    ========================

    Amazon EC2 C7a instances, powered by 4th generation AMD EPYC processors, deliver up to 50% higher performance compared to C6a instances.
    These instances support AVX-512, VNNI, and bfloat16, which enable support for more workloads, use Double Data Rate 5 (DDR5)
    to enable high-speed access to data in memory, and deliver 2.25x more memory bandwidth compared to C6a instances.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    These instances are ideal for high performance computing (HPC), batch processing, electronic design automation (EDA), gaming,
    video encoding, scientific modeling, distributed analytics, CPU-based machine learning (ML) inference, and ad-serving.

    """

    @classmethod
    def c7a_large(cls):
        return EmrInstanceType("c7a.large", 2, 4)

    @classmethod
    def c7a_xlarge(cls):
        return EmrInstanceType("c7a.xlarge", 4, 7.5, 8)

    @classmethod
    def c7a_2xlarge(cls):
        return EmrInstanceType("c7a.2xlarge", 8, 15.25, 16)

    @classmethod
    def c7a_4xlarge(cls):
        return EmrInstanceType("c7a.4xlarge", 16, 30.5, 32)

    @classmethod
    def c7a_8xlarge(cls):
        return EmrInstanceType("c7a.8xlarge", 32, 61, 64)

    @classmethod
    def c7a_12xlarge(cls):
        return EmrInstanceType("c7a.12xlarge", 48, 91.5, 96)

    @classmethod
    def c7a_16xlarge(cls):
        return EmrInstanceType("c7a.16xlarge", 64, 122, 128)
