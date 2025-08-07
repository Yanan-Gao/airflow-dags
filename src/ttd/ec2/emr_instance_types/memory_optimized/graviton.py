from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r8g import R8g


class MemoryOptimisedGraviton:

    @classmethod
    def g_xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_xlarge(),
                R7g.r7g_xlarge(),
                R6g.r6g_xlarge(),
            ],
            [
                R5.r5_xlarge(),
            ],
        )

    @classmethod
    def g_2xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_2xlarge(),
                R7g.r7g_2xlarge(),
                R6g.r6g_2xlarge(),
            ],
            [
                R5.r5_2xlarge(),
            ],
        )

    @classmethod
    def g_4xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_4xlarge(),
                R7g.r7g_4xlarge(),
                R6g.r6g_4xlarge(),
            ],
            [
                R5.r5_4xlarge(),
            ],
        )

    @classmethod
    def g_8xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_8xlarge(),
                R7g.r7g_8xlarge(),
                R6g.r6g_8xlarge(),
            ],
            [
                R5.r5_8xlarge(),
            ],
        )

    @classmethod
    def g_12xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_12xlarge(),
                R7g.r7g_12xlarge(),
                R6g.r6g_12xlarge(),
            ],
            [
                R5.r5_12xlarge(),
            ],
        )

    @classmethod
    def g_16xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_16xlarge(),
                R7g.r7g_16xlarge(),
                R6g.r6g_16xlarge(),
            ],
            [
                R5.r5_16xlarge(),
            ],
        )

    @classmethod
    def g_24xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R8g.r8g_24xlarge(),
            ],
            [
                R5.r5_24xlarge(),
            ],
        )

    @classmethod
    def g_48xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass([
            R8g.r8g_48xlarge(),
        ], )


RgClass = MemoryOptimisedGraviton
