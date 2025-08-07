from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.compute_optimized.c6g import C6g
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.ec2.emr_instance_types.compute_optimized.c8g import C8g


class ComputeOptimisedGraviton:

    @classmethod
    def c_xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_xlarge(),
                C7g.c7g_xlarge(),
                C6g.c6g_xlarge(),
            ],
            [
                C5.c5_xlarge(),
            ],
        )

    @classmethod
    def c_2xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_2xlarge(),
                C7g.c7g_2xlarge(),
                C6g.c6g_2xlarge(),
            ],
            [
                C5.c5_2xlarge(),
            ],
        )

    @classmethod
    def c_4xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_4xlarge(),
                C7g.c7g_4xlarge(),
                C6g.c6g_4xlarge(),
            ],
            [
                C5.c5_4xlarge(),
            ],
        )

    @classmethod
    def c_8xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_8xlarge(),
                C7g.c7g_8xlarge(),
                C6g.c6g_8xlarge(),
            ],
            [
                C5.c5_9xlarge(),
            ],
        )

    @classmethod
    def c_12xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_12xlarge(),
                C7g.c7g_12xlarge(),
                C6g.c6g_12xlarge(),
            ],
            [
                C5.c5_12xlarge(),
            ],
        )

    @classmethod
    def c_16xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_16xlarge(),
                C7g.c7g_16xlarge(),
                C6g.c6g_16xlarge(),
            ],
            [
                C5.c5_18xlarge(),
            ],
        )

    @classmethod
    def c_24xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                C8g.c8g_24xlarge(),
            ],
            [
                C5.c5_24xlarge(),
            ],
        )

    @classmethod
    def c_48xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass([
            C8g.c8g_48xlarge(),
        ], )


CgClass = ComputeOptimisedGraviton
