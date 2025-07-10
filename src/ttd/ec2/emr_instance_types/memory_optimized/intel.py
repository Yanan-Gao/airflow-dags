from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r6i import R6i
from ttd.ec2.emr_instance_types.memory_optimized.r7i import R7i


class MemoryOptimisedIntel:

    @classmethod
    def i_xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_xlarge(),
                R6i.r6i_xlarge(),
            ],
            [
                R5.r5_xlarge(),
            ],
        )

    @classmethod
    def i_2xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_2xlarge(),
                R6i.r6i_2xlarge(),
            ],
            [
                R5.r5_2xlarge(),
            ],
        )

    @classmethod
    def i_4xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_4xlarge(),
                R6i.r6i_4xlarge(),
            ],
            [
                R5.r5_4xlarge(),
            ],
        )

    @classmethod
    def i_8xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_8xlarge(),
                R6i.r6i_8xlarge(),
            ],
            [
                R5.r5_8xlarge(),
            ],
        )

    @classmethod
    def i_12xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_12xlarge(),
                R6i.r6i_12xlarge(),
            ],
            [
                R5.r5_12xlarge(),
            ],
        )

    @classmethod
    def i_16xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_16xlarge(),
                R6i.r6i_16xlarge(),
            ],
            [
                R5.r5_16xlarge(),
            ],
        )

    @classmethod
    def i_24xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass(
            [
                R7i.r7i_24xlarge(),
                R6i.r6i_24xlarge(),
            ],
            [
                R5.r5_24xlarge(),
            ],
        )

    @classmethod
    def i_32xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass([
            R6i.r6i_32xlarge(),
        ], )

    @classmethod
    def i_48xlarge(cls) -> EmrInstanceClass:
        return EmrInstanceClass([
            R7i.r7i_48xlarge(),
        ], )


RiClass = MemoryOptimisedIntel
