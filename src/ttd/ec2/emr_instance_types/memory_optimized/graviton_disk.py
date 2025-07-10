from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.memory_optimized.r8gd import R8gd


class MemoryOptimisedGravitonDisk:

    @classmethod
    def gd_xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_xlarge(),
                R7gd.r7gd_xlarge(),
                R6gd.r6gd_xlarge(),
            ],
            [
                R5d.r5d_xlarge(),
            ],
        )

    @classmethod
    def gd_2xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_2xlarge(),
                R7gd.r7gd_2xlarge(),
                R6gd.r6gd_2xlarge(),
            ],
            [
                R5d.r5d_2xlarge(),
            ],
        )

    @classmethod
    def gd_4xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_4xlarge(),
                R7gd.r7gd_4xlarge(),
                R6gd.r6gd_4xlarge(),
            ],
            [
                R5d.r5d_4xlarge(),
            ],
        )

    @classmethod
    def gd_8xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_8xlarge(),
                R7gd.r7gd_8xlarge(),
                R6gd.r6gd_8xlarge(),
            ],
            [
                R5d.r5d_8xlarge(),
            ],
        )

    @classmethod
    def gd_12xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_12xlarge(),
                R7gd.r7gd_12xlarge(),
                R6gd.r6gd_12xlarge(),
            ],
            [
                R5d.r5d_12xlarge(),
            ],
        )

    @classmethod
    def gd_16xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_16xlarge(),
                R7gd.r7gd_16xlarge(),
                R6gd.r6gd_16xlarge(),
            ],
            [
                R5d.r5d_16xlarge(),
            ],
        )

    @classmethod
    def gd_24xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8gd.r8gd_24xlarge(),
            ],
            [
                R5d.r5d_24xlarge(),
            ],
        )

    @classmethod
    def gd_48xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass([
            R8gd.r8gd_48xlarge(),
        ], )


RgdClass = MemoryOptimisedGravitonDisk
