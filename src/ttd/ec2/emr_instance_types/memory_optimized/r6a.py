from ttd.ec2.emr_instance_type import EmrInstanceType


class R6a:
    """
    Amazon EC2 R6a Instances
    ========================

    Amazon EC2 R6a instances are Memory Optimized instances that accelerate
    performance for workloads that process large data sets in memory. R6a instances deliver up to 10% cost savings over comparable
    instance types.

    Ideal Use cases: These instances are SAP-Certified and are ideal for memory-intensive workloads such as backend servers supporting
    enterprise applications (e.g. Microsoft Exchange and SharePoint, SAP Business Suite, MySQL, Microsoft SQL Server, and PostgreSQL
    databases), gaming servers, caching fleets, as well as for application development environments.

    Key technology:
    ^^^^^^^^^^^^^^^
    - Up to 3.6 GHz 3rd generation AMD EPYC processors
    - Up to 192 vCPUs
    - Up to 1,536 GiB of memory
    - Up to 50 Gbps network bandwidth
    - Up to 40 Gbps EBS bandwidth
    """

    @classmethod
    def r6a_large(cls):
        return EmrInstanceType("r6a.large", 2, 16)

    @classmethod
    def r6a_xlarge(cls):
        return EmrInstanceType("r6a.xlarge", 4, 32)

    @classmethod
    def r6a_2xlarge(cls):
        return EmrInstanceType("r6a.2xlarge", 8, 64)

    @classmethod
    def r6a_4xlarge(cls):
        return EmrInstanceType("r6a.4xlarge", 16, 128)

    @classmethod
    def r6a_8xlarge(cls):
        return EmrInstanceType("r6a.8xlarge", 32, 256)

    @classmethod
    def r6a_12xlarge(cls):
        return EmrInstanceType("r6a.12xlarge", 48, 384)

    @classmethod
    def r6a_16xlarge(cls):
        return EmrInstanceType("r6a.16xlarge", 64, 512)

    @classmethod
    def r6a_24xlarge(cls):
        return EmrInstanceType("r6a.24xlarge", 96, 768)

    @classmethod
    def r6a_32xlarge(cls):
        return EmrInstanceType("r6a.32xlarge", 128, 1024)

    @classmethod
    def r6a_48xlarge(cls):
        return EmrInstanceType("r6a.48xlarge", 192, 1536)
