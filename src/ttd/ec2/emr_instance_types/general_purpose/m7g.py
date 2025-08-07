from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import SsdAwsInstanceStorage, AwsStorageVolume, EbsAwsInstanceStorage
from ttd.ec2.emr_instance_type import EmrInstanceType


class M7g:

    # m7g
    @classmethod
    def m7g_medium(cls):
        return EmrInstanceType(
            "m7g.medium", 1, 4, 4, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7g_large(cls):
        return EmrInstanceType(
            "m7g.large", 2, 8, 8, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7g_xlarge(cls):
        return EmrInstanceType(
            "m7g.xlarge", 4, 15.25, 16, EbsAwsInstanceStorage(), AwsInstanceBandwidth(12.5, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7g_2xlarge(cls):
        return EmrInstanceType(
            "m7g.2xlarge", 8, 30.5, 32, EbsAwsInstanceStorage(), AwsInstanceBandwidth(15, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7g_4xlarge(cls):
        return EmrInstanceType(
            "m7g.4xlarge", 16, 61, 64, EbsAwsInstanceStorage(), AwsInstanceBandwidth(15, True), AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7g_8xlarge(cls):
        return EmrInstanceType("m7g.8xlarge", 32, 122, 128, EbsAwsInstanceStorage(), AwsInstanceBandwidth(15), AwsInstanceBandwidth(10))

    @classmethod
    def m7g_12xlarge(cls):
        return EmrInstanceType("m7g.12xlarge", 48, 183, 192, EbsAwsInstanceStorage(), AwsInstanceBandwidth(22.5), AwsInstanceBandwidth(15))

    @classmethod
    def m7g_16xlarge(cls):
        return EmrInstanceType("m7g.16xlarge", 64, 244, 256, EbsAwsInstanceStorage(), AwsInstanceBandwidth(30), AwsInstanceBandwidth(20))

    # m7gd
    @classmethod
    def m7gd_medium(cls):
        return EmrInstanceType(
            "m7gd.medium", 1, 4, 4, SsdAwsInstanceStorage([AwsStorageVolume(59, "NVMe SSD")]), AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7gd_large(cls):
        return EmrInstanceType(
            "m7gd.large", 2, 8, 8, SsdAwsInstanceStorage([AwsStorageVolume(118, "NVMe SSD")]), AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7gd_xlarge(cls):
        return EmrInstanceType(
            "m7gd.xlarge", 4, 15.25, 16, SsdAwsInstanceStorage([AwsStorageVolume(237, "NVMe SSD")]), AwsInstanceBandwidth(12.5, True),
            AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7gd_2xlarge(cls):
        return EmrInstanceType(
            "m7gd.2xlarge", 8, 30.5, 32, SsdAwsInstanceStorage([AwsStorageVolume(474, "NVMe SSD")]), AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7gd_4xlarge(cls):
        return EmrInstanceType(
            "m7gd.4xlarge", 16, 61, 64, SsdAwsInstanceStorage([AwsStorageVolume(950, "NVMe SSD")]), AwsInstanceBandwidth(15, True),
            AwsInstanceBandwidth(10, True)
        )

    @classmethod
    def m7gd_8xlarge(cls):
        return EmrInstanceType(
            "m7gd.8xlarge", 32, 122, 128, SsdAwsInstanceStorage([AwsStorageVolume(1900, "NVMe SSD")]), AwsInstanceBandwidth(15),
            AwsInstanceBandwidth(10)
        )

    @classmethod
    def m7gd_12xlarge(cls):
        return EmrInstanceType(
            "m7gd.12xlarge", 48, 183, 192, SsdAwsInstanceStorage([AwsStorageVolume(1425, "NVMe SSD"),
                                                                  AwsStorageVolume(1425, "NVMe SSD")]), AwsInstanceBandwidth(22.5),
            AwsInstanceBandwidth(15)
        )

    @classmethod
    def m7gd_16xlarge(cls):
        return EmrInstanceType(
            "m7gd.16xlarge", 64, 244, 256, SsdAwsInstanceStorage([AwsStorageVolume(1900, "NVMe SSD"),
                                                                  AwsStorageVolume(1900, "NVMe SSD")]), AwsInstanceBandwidth(30),
            AwsInstanceBandwidth(20)
        )
