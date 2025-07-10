from ttd.ec2.emr_instance_type import EmrInstanceType


class C6g:
    """
    Amazon EC2 C6g instances are powered by Arm-based AWS Graviton2 processors. They deliver up to 40% better price performance over C5
    instances and are ideal for running advanced compute-intensive workloads. This includes workloads such as high performance
    computing (HPC), batch processing, ad serving, video encoding, gaming, scientific modelling, distributed analytics, and CPU-based
    machine learning inference.

    C6g instances are available with local NVMe-based SSD block-level storage option (C6gd) for applications that need high-speed,
    low latency local storage. C6g instances with 100 Gbps networking and Elastic Fabric Adapter (EFA) support, called the C6gn instances,
    are also available for applications that need higher networking throughput, such as high performance computing (HPC),
    network appliance, real-time video communication, and data analytics.
    """

    ## Not supported by EMR
    # @classmethod
    # def c6g_large(cls):
    #     return EmrInstanceType("c6g.large", 2, 4, 4)

    @classmethod
    def c6g_xlarge(cls):
        return EmrInstanceType("c6g.xlarge", 4, 7.5, 8)

    @classmethod
    def c6g_2xlarge(cls):
        return EmrInstanceType("c6g.2xlarge", 8, 15.25, 16)

    @classmethod
    def c6g_4xlarge(cls):
        return EmrInstanceType("c6g.4xlarge", 16, 30.5, 32)

    @classmethod
    def c6g_8xlarge(cls):
        return EmrInstanceType("c6g.8xlarge", 32, 61, 64)

    @classmethod
    def c6g_12xlarge(cls):
        return EmrInstanceType("c6g.12xlarge", 48, 91.5, 96)

    @classmethod
    def c6g_16xlarge(cls):
        return EmrInstanceType("c6g.16xlarge", 64, 122, 128)

    ## not supported by EMR
    # @classmethod
    # def c6gd_large(cls):
    #     return EmrInstanceType("c6gd.large", 2, 4)

    @classmethod
    def c6gd_xlarge(cls):
        return EmrInstanceType("c6gd.xlarge", 4, 7.5, 8)

    @classmethod
    def c6gd_2xlarge(cls):
        return EmrInstanceType("c6gd.2xlarge", 8, 15.25, 16)

    @classmethod
    def c6gd_4xlarge(cls):
        return EmrInstanceType("c6gd.4xlarge", 16, 30.5, 32)

    @classmethod
    def c6gd_8xlarge(cls):
        return EmrInstanceType("c6gd.8xlarge", 32, 61, 64)

    @classmethod
    def c6gd_12xlarge(cls):
        return EmrInstanceType("c6gd.12xlarge", 48, 91.5, 96)

    @classmethod
    def c6gd_16xlarge(cls):
        return EmrInstanceType("c6gd.16xlarge", 64, 122, 128)

    @classmethod
    def c6gn_xlarge(cls):
        return EmrInstanceType("c6gn.xlarge", 4, 7.5, 8)

    @classmethod
    def c6gn_2xlarge(cls):
        return EmrInstanceType("c6gn.2xlarge", 8, 15.25, 16)

    @classmethod
    def c6gn_4xlarge(cls):
        return EmrInstanceType("c6gn.4xlarge", 16, 30.5, 32)

    @classmethod
    def c6gn_8xlarge(cls):
        return EmrInstanceType("c6gn.8xlarge", 32, 61, 64)

    @classmethod
    def c6gn_12xlarge(cls):
        return EmrInstanceType("c6gn.12xlarge", 48, 91.5, 96)

    @classmethod
    def c6gn_16xlarge(cls):
        return EmrInstanceType("c6gn.16xlarge", 64, 122, 128)
