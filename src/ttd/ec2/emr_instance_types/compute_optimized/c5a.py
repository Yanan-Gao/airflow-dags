from ttd.ec2.emr_instance_type import EmrInstanceType


class C5a:
    """
    Amazon EC2 C5 Instances
    ^^^^^^^^^^^^^^^^^^^^^^^
    Amazon EC2 C5 instances are compute optimized instances that deliver cost-effective high performance at a low price per compute
    ratio for compute-bound workloads.

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    High performance web servers, scientific modeling, batch processing, distributed analytics, high-performance computing (HPC),
    machine/deep learning inference, ad serving, highly scalable multiplayer gaming, and video encoding.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Custom 2nd generation Intel Xeon Scalable processors (Cascade Lake) with a sustained all core Turbo frequency of 3.6 GHz and single
      core Turbo frequency of up to 3.9 GHz or 1st generation Intel Xeon Platinum 8000 series (Skylake-SP) processor with a sustained all
      core Turbo frequency of up to 3.4 GHz, and single core Turbo frequency of up to 3.5 GHz.
    - Up to 96 vCPUs
    - Up to 192 GiB of memory
    - Up to 25 Gbps network bandwidth

    `Docs <https://aws.amazon.com/ec2/instance-types/c5/>`_.

    C5a and C5ad instances offer leading x86 price/performance for a broad set of compute-intensive workloads and feature 2nd
    generation 3.3GHz AMD EPYC 7002 series processors built on a 7nm process node for increased efficiency. C5a and C5ad instances
    provide up to 20 Gbps of network bandwidth and 9.5 Gbps of dedicated bandwidth to Amazon EBS. With C5ad instances, local NVMe-based
    SSDs are physically connected to the host server and provide block-level storage that is coupled to the lifetime of the instance.
    """

    @classmethod
    def c5a_large(cls):
        return EmrInstanceType("c5a.large", 2, 4)

    @classmethod
    def c5a_xlarge(cls):
        return EmrInstanceType("c5a.xlarge", 4, 8)

    @classmethod
    def c5a_2xlarge(cls):
        return EmrInstanceType("c5a.2xlarge", 8, 16)

    @classmethod
    def c5a_4xlarge(cls):
        return EmrInstanceType("c5a.4xlarge", 16, 32)

    @classmethod
    def c5a_8xlarge(cls):
        return EmrInstanceType("c5a.8xlarge", 32, 64)

    @classmethod
    def c5a_12xlarge(cls):
        return EmrInstanceType("c5a.12xlarge", 48, 96)

    @classmethod
    def c5a_16xlarge(cls):
        return EmrInstanceType("c5a.16xlarge", 64, 128)

    @classmethod
    def c5a_24xlarge(cls):
        return EmrInstanceType("c5a.24xlarge", 96, 192)

    @classmethod
    def c5ad_xlarge(cls):
        return EmrInstanceType("c5ad.xlarge", 4, 8)

    @classmethod
    def c5ad_2xlarge(cls):
        return EmrInstanceType("c5ad.2xlarge", 8, 16)

    @classmethod
    def c5ad_4xlarge(cls):
        return EmrInstanceType("c5ad.4xlarge", 16, 32)

    @classmethod
    def c5ad_8xlarge(cls):
        return EmrInstanceType("c5ad.8xlarge", 32, 64)

    @classmethod
    def c5ad_12xlarge(cls):
        return EmrInstanceType("c5ad.12xlarge", 48, 96)

    @classmethod
    def c5ad_16xlarge(cls):
        return EmrInstanceType("c5ad.16xlarge", 64, 128)

    @classmethod
    def c5ad_24xlarge(cls):
        return EmrInstanceType("c5ad.24xlarge", 96, 192)
