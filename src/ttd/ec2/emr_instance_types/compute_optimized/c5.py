from ttd.ec2.emr_instance_type import EmrInstanceType


class C5:
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
    """

    @classmethod
    def c5_xlarge(cls):
        return EmrInstanceType("c5.xlarge", 4, 8)

    @classmethod
    def c5_2xlarge(cls):
        return EmrInstanceType("c5.2xlarge", 8, 16)

    @classmethod
    def c5_4xlarge(cls):
        return EmrInstanceType("c5.4xlarge", 16, 32)

    @classmethod
    def c5_9xlarge(cls):
        return EmrInstanceType("c5.9xlarge", 36, 72)

    @classmethod
    def c5_12xlarge(cls):
        return EmrInstanceType("c5.12xlarge", 48, 96)

    @classmethod
    def c5_18xlarge(cls):
        return EmrInstanceType("c5.18xlarge", 72, 144)

    @classmethod
    def c5_24xlarge(cls):
        return EmrInstanceType("c5.24xlarge", 96, 192)

    # Compute optimased NVMe based instances
    @classmethod
    def c5d_4xlarge(cls):
        return EmrInstanceType("c5d.4xlarge", 16, 32)

    @classmethod
    def c5d_9xlarge(cls):
        return EmrInstanceType("c5d.9xlarge", 36, 72)

    @classmethod
    def c5d_12xlarge(cls):
        return EmrInstanceType("c5d.12xlarge", 48, 96)

    @classmethod
    def c5d_18xlarge(cls):
        return EmrInstanceType("c5d.18xlarge", 72, 144)
