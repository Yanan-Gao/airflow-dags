from ttd.ec2.emr_instance_type import EmrInstanceType


class M6g:
    """
    Amazon EC2 M6g Instances
    ^^^^^^^^^^^^^^^^^^^^^^^^

    Amazon EC2 M6g instances are general purpose instances powered by Arm-based AWS Graviton2 processors. They deliver up to 40% better
    price performance over current generation M5 instances and offer a balance of compute, memory, and networking resources for a broad
    set of workloads.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    Applications built on open-source software such as application servers, microservices, gaming servers, mid-size data stores,
    and caching fleets

    Key technology:
    ^^^^^^^^^^^^^^^
    - Custom built AWS Graviton2 processor with 64-bit Arm Neoverse cores
    - Up to 64 vCPUs
    - Up to 256 GiB of memory
    - Up to 25 Gbps network bandwidth
    """

    @classmethod
    def m6g_xlarge(cls):
        return EmrInstanceType("m6g.xlarge", 4, 15)  # 15.3 instead of 16 from EMR console

    @classmethod
    def m6g_2xlarge(cls):
        return EmrInstanceType("m6g.2xlarge", 8, 30)  # 30.5 instead of 32 from EMR console

    @classmethod
    def m6g_4xlarge(cls):
        return EmrInstanceType("m6g.4xlarge", 16, 61)  # 61 instead of 64 from EMR console

    @classmethod
    def m6g_8xlarge(cls):
        return EmrInstanceType("m6g.8xlarge", 32, 122)  # 122 instead of 128 from EMR console

    @classmethod
    def m6g_12xlarge(cls):
        return EmrInstanceType("m6g.12xlarge", 48, 185)  # 185 instead of 192 from EMR console

    @classmethod
    def m6g_16xlarge(cls):
        return EmrInstanceType("m6g.16xlarge", 64, 244)  # 244 instead of 256 from EMR console

    # m6gd

    @classmethod
    def m6gd_xlarge(cls):
        return EmrInstanceType("m6gd.xlarge", 4, 15)  # 15.3 instead of 16 from EMR console

    @classmethod
    def m6gd_2xlarge(cls):
        return EmrInstanceType("m6gd.2xlarge", 8, 30)  # 30.5 instead of 32 from EMR console

    @classmethod
    def m6gd_4xlarge(cls):
        return EmrInstanceType("m6gd.4xlarge", 16, 61)  # 61 instead of 64 from EMR console

    @classmethod
    def m6gd_8xlarge(cls):
        return EmrInstanceType("m6gd.8xlarge", 32, 122)  # 122 instead of 128 from EMR console

    @classmethod
    def m6gd_12xlarge(cls):
        return EmrInstanceType("m6gd.12xlarge", 48, 185)  # 185 instead of 192 from EMR console

    @classmethod
    def m6gd_16xlarge(cls):
        return EmrInstanceType("m6gd.16xlarge", 64, 244)  # 244 instead of 256 from EMR console
