from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import EbsAwsInstanceStorage
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.emr_version import EmrVersion


class R8g:
    """
    Amazon EC2 r8g Instances
    =========================

    Amazon EC2 R8g instances are powered by AWS Graviton4 processors.
    R8g instances deliver up to 30% better performance over Graviton3-based R7g instances

    Features:
    Powered by custom-built AWS Graviton4 processors
    Larger instance sizes with up to 3x more vCPUs and memory than R7g instances
    Features the latest DDR5-5600 memory
    Optimized for Amazon EBS by default
    Supports Elastic Fabric Adapter (EFA) on r8g.24xlarge, r8g.48xlarge, r8g.metal-24xl, and r8g.metal-48xl
    Powered by the AWS Nitro System, a combination of dedicated hardware and lightweight hypervisor

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    Open-source databases, in-memory caches, and real time big data analytics.
    """

    minimum_emr_versions = [
        EmrVersion("emr-5.36.1"),
        EmrVersion("emr-6.10.0"),
        EmrVersion("emr-7.0.0"),
    ]

    ## not supported by EMR
    # @classmethod
    # def r8g_large(cls):
    #     return EmrInstanceType(
    #         "r8g.large",
    #         2,
    #         15.25,
    #         16,
    #         EbsAwsInstanceStorage(),
    #         AwsInstanceBandwidth(12.5, True),
    #         AwsInstanceBandwidth(10, True),
    #         cls.minimum_emr_versions,
    #     )

    @classmethod
    def r8g_xlarge(cls):
        return EmrInstanceType(
            "r8g.xlarge",
            4,
            30.5,
            32,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_2xlarge(cls):
        return EmrInstanceType(
            "r8g.2xlarge",
            8,
            61,
            64,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_4xlarge(cls):
        return EmrInstanceType(
            "r8g.4xlarge",
            16,
            122,
            128,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_8xlarge(cls):
        return EmrInstanceType(
            "r8g.8xlarge",
            32,
            244,
            256,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(15),
            AwsInstanceBandwidth(10),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_12xlarge(cls):
        return EmrInstanceType(
            "r8g.12xlarge",
            48,
            366,
            384,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(22.5),
            AwsInstanceBandwidth(15),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_16xlarge(cls):
        return EmrInstanceType(
            "r8g.16xlarge",
            64,
            488,
            512,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(30),
            AwsInstanceBandwidth(20),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_24xlarge(cls):
        return EmrInstanceType(
            "r8g.24xlarge",
            96,
            732,
            768,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(40),
            AwsInstanceBandwidth(30),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8g_48xlarge(cls):
        return EmrInstanceType(
            "r8g.48xlarge",
            192,
            1464,
            1536,
            EbsAwsInstanceStorage(),
            AwsInstanceBandwidth(50),
            AwsInstanceBandwidth(40),
            cls.minimum_emr_versions,
        )
