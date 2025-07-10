from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import SsdAwsInstanceStorage, AwsStorageVolume
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.emr_version import EmrVersion


class R8gd:
    """
    Amazon EC2 r8gdd Instances
    =========================

    Amazon EC2 R8g instances are powered by AWS Graviton4 processors.
    R8g instances deliver up to 30% better performance over Graviton3-based R7g instances

    Features:
    Powered by custom-built AWS Graviton4 processors
    Larger instance sizes with up to 3x more vCPUs and memory than R7g instances
    Features the latest DDR5-5600 memory
    Optimized for Amazon EBS by default
    Supports Elastic Fabric Adapter (EFA) on r8gd.24xlarge, r8gd.48xlarge, r8gd.metal-24xl, and r8gd.metal-48xl
    Powered by the AWS Nitro System, a combination of dedicated hardware and lightweight hypervisor

    Ideal Use cases:
    ^^^^^^^^^^^^^^^^
    Open-source databases, in-memory caches, and real time big data analytics.
    """

    minimum_emr_versions = [
        EmrVersion("emr-5.36.2"),
        EmrVersion("emr-6.15.0"),
        EmrVersion("emr-7.1.0"),
    ]

    ## not supported by EMR
    # @classmethod
    # def r8gd_large(cls):
    #     return EmrInstanceType(
    #         "r8gd.large",
    #         2,
    #         15.25,
    #         16,
    #         SsdAwsInstanceStorage([AwsStorageVolume(118, "NVMe SSD")]),
    #         AwsInstanceBandwidth(12.5, True),
    #         AwsInstanceBandwidth(10, True),
    #         cls.minimum_emr_versions,
    #     )

    @classmethod
    def r8gd_xlarge(cls):
        return EmrInstanceType(
            "r8gd.xlarge",
            4,
            30.5,
            32,
            SsdAwsInstanceStorage([AwsStorageVolume(237, "NVMe SSD")]),
            AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_2xlarge(cls):
        return EmrInstanceType(
            "r8gd.2xlarge",
            8,
            61,
            64,
            SsdAwsInstanceStorage([AwsStorageVolume(474, "NVMe SSD")]),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_4xlarge(cls):
        return EmrInstanceType(
            "r8gd.4xlarge",
            16,
            122,
            128,
            SsdAwsInstanceStorage([AwsStorageVolume(950, "NVMe SSD")]),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_8xlarge(cls):
        return EmrInstanceType(
            "r8gd.8xlarge",
            32,
            244,
            256,
            SsdAwsInstanceStorage([AwsStorageVolume(1900, "NVMe SSD")]),
            AwsInstanceBandwidth(15),
            AwsInstanceBandwidth(10),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_12xlarge(cls):
        return EmrInstanceType(
            "r8gd.12xlarge",
            48,
            366,
            384,
            SsdAwsInstanceStorage([
                AwsStorageVolume(950, "NVMe SSD"),
                AwsStorageVolume(950, "NVMe SSD"),
                AwsStorageVolume(950, "NVMe SSD"),
            ], ),
            AwsInstanceBandwidth(22.5),
            AwsInstanceBandwidth(15),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_16xlarge(cls):
        return EmrInstanceType(
            "r8gd.16xlarge",
            64,
            488,
            512,
            SsdAwsInstanceStorage([
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
            ], ),
            AwsInstanceBandwidth(30),
            AwsInstanceBandwidth(20),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_24xlarge(cls):
        return EmrInstanceType(
            "r8gd.24xlarge",
            96,
            732,
            768,
            SsdAwsInstanceStorage([
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
            ], ),
            AwsInstanceBandwidth(40),
            AwsInstanceBandwidth(30),
            cls.minimum_emr_versions,
        )

    @classmethod
    def r8gd_48xlarge(cls):
        return EmrInstanceType(
            "r8gd.48xlarge",
            192,
            1464,
            1536,
            SsdAwsInstanceStorage([
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
                AwsStorageVolume(1900, "NVMe SSD"),
            ], ),
            AwsInstanceBandwidth(50),
            AwsInstanceBandwidth(40),
            cls.minimum_emr_versions,
        )
