from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import EbsAwsInstanceStorage
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.emr_version import EmrVersion


class C8g:
    """
    Amazon EC2 c8g.Instances
    =========================

    Amazon EC2 C8g instances are powered by AWS Graviton4 processors. They deliver the best price performance in Amazon EC2 for compute-intensive workloads.

    Features:
    * Powered by custom-built AWS Graviton4 processors
    * Larger instance sizes with up to 3x more vCPUs and memory than C7g instances
    * Features the latest DDR5-5600 memory
    * Optimized for Amazon EBS by default
    * Instance storage offered via EBS or NVMe SSDs that are physically attached to the host server
    * With C8gd instances, local NVMe-based SSDs are physically connected to the host server and provide block-level
        storage that is coupled to the lifetime of the instance
    * Supports Elastic Fabric Adapter (EFA) on c8g.24xlarge, c8g.48xlarge, c8g.metal-24xl, c8g.metal-48xl,
        c8gd.24xlarge, c8gd.48xlarge, c8gd.metal-24xl, and c8gd.metal-48xl
    * Powered by the AWS Nitro System, a combination of dedicated hardware and lightweight hypervisor
    """

    minimum_emr_versions = [
        EmrVersion("emr-5.36.1"),
        EmrVersion("emr-6.10.0"),
        EmrVersion("emr-7.0.0"),
    ]

    ## not supported by EMR
    # @classmethod
    # def c8g_large(cls):
    #     return EmrInstanceType(
    #         "c8g.large",
    #         2,
    #         15.25,
    #         16,
    #         EbsAwsInstanceStorage(),
    #         AwsInstanceBandwidth(12.5, True),
    #         AwsInstanceBandwidth(10, True),
    #         cls.minimum_emr_versions,
    #     )

    @classmethod
    def c8g_xlarge(cls):
        return EmrInstanceType(
            "c8g.xlarge",
            4,
            7.5,
            8,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_2xlarge(cls):
        return EmrInstanceType(
            "c8g.2xlarge",
            8,
            15.25,
            16,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_4xlarge(cls):
        return EmrInstanceType(
            "c8g.4xlarge",
            16,
            30.5,
            32,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_8xlarge(cls):
        return EmrInstanceType(
            "c8g.8xlarge",
            32,
            61,
            64,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(15),
            AwsInstanceBandwidth(10),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_12xlarge(cls):
        return EmrInstanceType(
            "c8g.12xlarge",
            48,
            91.5,
            96,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(22.5),
            AwsInstanceBandwidth(15),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_16xlarge(cls):
        return EmrInstanceType(
            "c8g.16xlarge",
            64,
            122,
            128,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(30),
            AwsInstanceBandwidth(20),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_24xlarge(cls):
        return EmrInstanceType(
            "c8g.24xlarge",
            96,
            183,
            192,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(40),
            AwsInstanceBandwidth(30),
            cls.minimum_emr_versions,
        )

    @classmethod
    def c8g_48xlarge(cls):
        return EmrInstanceType(
            "c8g.48xlarge",
            192,
            366,
            384,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(50),
            AwsInstanceBandwidth(40),
            cls.minimum_emr_versions,
        )
