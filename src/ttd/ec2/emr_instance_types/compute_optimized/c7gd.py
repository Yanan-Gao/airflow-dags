from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import SsdAwsInstanceStorage, AwsStorageVolume
from ttd.ec2.emr_instance_type import EmrInstanceType


class C7gd:
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
    # def c7gd_large(cls):
    #     return EmrInstanceType(
    #         "c7gd.large", 2, 4, 4, SsdAwsInstanceStorage([AwsStorageVolume(118, "NVMe SSD")]), AwsInstanceBandwidth(12.5, True),
    #         AwsInstanceBandwidth(10, True)
    #     )

    @classmethod
    def c7gd_xlarge(cls):
        return EmrInstanceType(
            "c7gd.xlarge",
            4,
            7.5,
            8,
            SsdAwsInstanceStorage([AwsStorageVolume(237, "NVMe SSD")]),
            AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True),
        )

    @classmethod
    def c7gd_2xlarge(cls):
        return EmrInstanceType(
            "c7gd.2xlarge",
            8,
            15.25,
            16,
            SsdAwsInstanceStorage([AwsStorageVolume(474, "NVMe SSD")]),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
        )

    @classmethod
    def c7gd_4xlarge(cls):
        return EmrInstanceType(
            "c7gd.4xlarge",
            16,
            30.5,
            32,
            SsdAwsInstanceStorage([AwsStorageVolume(950, "NVMe SSD")]),
            AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True),
        )

    @classmethod
    def c7gd_8xlarge(cls):
        return EmrInstanceType(
            "c7gd.8xlarge",
            32,
            61,
            64,
            SsdAwsInstanceStorage([AwsStorageVolume(1900, "NVMe SSD")]),
            AwsInstanceBandwidth(15),
            AwsInstanceBandwidth(10),
        )

    @classmethod
    def c7gd_12xlarge(cls):
        return EmrInstanceType(
            "c7gd.12xlarge",
            48,
            91.5,
            96,
            SsdAwsInstanceStorage([AwsStorageVolume(1425, "NVMe SSD"),
                                   AwsStorageVolume(1425, "NVMe SSD")], ),
            AwsInstanceBandwidth(22.5),
            AwsInstanceBandwidth(15),
        )

    @classmethod
    def c7gd_16xlarge(cls):
        return EmrInstanceType(
            "c7gd.16xlarge",
            64,
            122,
            128,
            SsdAwsInstanceStorage([AwsStorageVolume(1900, "NVMe SSD"),
                                   AwsStorageVolume(1900, "NVMe SSD")], ),
            AwsInstanceBandwidth(30),
            AwsInstanceBandwidth(20),
        )
