# Add more instance types to this class as needed
from ttd.alicloud.alicloud_instance_type import AliCloudInstanceType


class AliCloudInstanceTypes:
    # General Purpose
    @classmethod
    def ECS_G6_X(cls):
        return AliCloudInstanceType("ecs.g6.xlarge", cores=4, memory=16)

    @classmethod
    def ECS_G6_2X(cls):
        return AliCloudInstanceType("ecs.g6.2xlarge", cores=8, memory=32)

    @classmethod
    def ECS_G6_3X(cls):
        return AliCloudInstanceType("ecs.g6.3xlarge", cores=12, memory=48)

    @classmethod
    def ECS_G6_4X(cls):
        return AliCloudInstanceType("ecs.g6.4xlarge", cores=16, memory=64)

    @classmethod
    def ECS_G6_6X(cls):
        return AliCloudInstanceType("ecs.g6.6xlarge", cores=24, memory=96)

    @classmethod
    def ECS_G6_8X(cls):
        return AliCloudInstanceType("ecs.g6.8xlarge", cores=32, memory=128)

    @classmethod
    def ECS_G6_13X(cls):
        return AliCloudInstanceType("ecs.g6.13xlarge", cores=52, memory=192)

    @classmethod
    def ECS_G6_26X(cls):
        return AliCloudInstanceType("ecs.g6.26xlarge", cores=104, memory=384)

    @classmethod
    def ECS_G6E_X(cls):
        return AliCloudInstanceType("ecs.g6e.xlarge", cores=4, memory=16)

    @classmethod
    def ECS_G6E_2X(cls):
        return AliCloudInstanceType("ecs.g6e.2xlarge", cores=8, memory=32)

    @classmethod
    def ECS_G6E_4X(cls):
        return AliCloudInstanceType("ecs.g6e.4xlarge", cores=16, memory=64)

    @classmethod
    def ECS_G6E_8X(cls):
        return AliCloudInstanceType("ecs.g6e.8xlarge", cores=32, memory=128)

    @classmethod
    def ECS_G6E_13X(cls):
        return AliCloudInstanceType("ecs.g6e.13xlarge", cores=52, memory=192)

    @classmethod
    def ECS_G7_X(cls):
        return AliCloudInstanceType("ecs.g7.xlarge", cores=4, memory=16)

    @classmethod
    def ECS_G7_2X(cls):
        return AliCloudInstanceType("ecs.g7.2xlarge", cores=8, memory=32)

    @classmethod
    def ECS_G7_3X(cls):
        return AliCloudInstanceType("ecs.g7.3xlarge", cores=12, memory=48)

    @classmethod
    def ECS_G7_4X(cls):
        return AliCloudInstanceType("ecs.g7.4xlarge", cores=16, memory=64)

    @classmethod
    def ECS_G7_6X(cls):
        return AliCloudInstanceType("ecs.g7.6xlarge", cores=24, memory=96)

    @classmethod
    def ECS_G7_8X(cls):
        return AliCloudInstanceType("ecs.g7.8xlarge", cores=32, memory=128)

    # Computed Optimized
    @classmethod
    def ECS_C5_4X(cls):
        return AliCloudInstanceType("ecs.c5.4xlarge", cores=16, memory=32)

    @classmethod
    def ECS_C5_6X(cls):
        return AliCloudInstanceType("ecs.c5.6xlarge", cores=24, memory=48)

    @classmethod
    def ECS_C5_8X(cls):
        return AliCloudInstanceType("ecs.c5.8xlarge", cores=32, memory=64)

    @classmethod
    def ECS_C5_16X(cls):
        return AliCloudInstanceType("ecs.c6.2xlarge", cores=8, memory=16)

    @classmethod
    def ECS_C6_2X(cls):
        return AliCloudInstanceType("ecs.c5.4xlarge", cores=8, memory=16)

    @classmethod
    def ECS_C6_3X(cls):
        return AliCloudInstanceType("ecs.c6.3xlarge", cores=12, memory=24)

    @classmethod
    def ECS_C6_4X(cls):
        return AliCloudInstanceType("ecs.c6.4xlarge", cores=16, memory=32)

    @classmethod
    def ECS_C6_6X(cls):
        return AliCloudInstanceType("ecs.c6.6xlarge", cores=24, memory=48)

    @classmethod
    def ECS_C6_8X(cls):
        return AliCloudInstanceType("ecs.c6.8xlarge", cores=32, memory=64)

    @classmethod
    def ECS_C5_13X(cls):
        return AliCloudInstanceType("ecs.c5.13xlarge", cores=52, memory=96)

    @classmethod
    def ECS_C6E_2X(cls):
        return AliCloudInstanceType("ecs.c6e.2xlarge", cores=8, memory=16)

    @classmethod
    def ECS_C6E_4X(cls):
        return AliCloudInstanceType("ecs.c6e.4xlarge", cores=16, memory=32)

    @classmethod
    def ECS_C6E_8X(cls):
        return AliCloudInstanceType("ecs.c6e.8xlarge", cores=32, memory=64)

    @classmethod
    def ECS_C7_X(cls):
        return AliCloudInstanceType("ecs.c7.xlarge", cores=4, memory=8)

    @classmethod
    def ECS_C7_2X(cls):
        return AliCloudInstanceType("ecs.c7.2xlarge", cores=8, memory=16)

    @classmethod
    def ECS_C7_3X(cls):
        return AliCloudInstanceType("ecs.c7.3xlarge", cores=12, memory=24)

    @classmethod
    def ECS_C7_4X(cls):
        return AliCloudInstanceType("ecs.c7.4xlarge", cores=16, memory=32)

    @classmethod
    def ECS_C7_6X(cls):
        return AliCloudInstanceType("ecs.c7.6xlarge", cores=24, memory=48)

    @classmethod
    def ECS_C7_8X(cls):
        return AliCloudInstanceType("ecs.c7.8xlarge", cores=32, memory=64)

    # Memory Optimized
    @classmethod
    def ECS_R5_X(cls):
        return AliCloudInstanceType("ecs.r5.xlarge", cores=4, memory=32)

    @classmethod
    def ECS_R5_2X(cls):
        return AliCloudInstanceType("ecs.r5.2xlarge", cores=8, memory=64)

    @classmethod
    def ECS_R5_4X(cls):
        return AliCloudInstanceType("ecs.r5.4xlarge", cores=16, memory=128)

    @classmethod
    def ECS_R5_6X(cls):
        return AliCloudInstanceType("ecs.r5.6xlarge", cores=24, memory=192)

    @classmethod
    def ECS_R5_8X(cls):
        return AliCloudInstanceType("ecs.r5.8xlarge", cores=32, memory=256)

    @classmethod
    def ECS_R5_16X(cls):
        return AliCloudInstanceType("ecs.r5.16xlarge", cores=64, memory=512)

    @classmethod
    def ECS_R6_X(cls):
        return AliCloudInstanceType("ecs.r6.xlarge", cores=4, memory=32)

    @classmethod
    def ECS_R6_2X(cls):
        return AliCloudInstanceType("ecs.r6.2xlarge", cores=8, memory=64)

    @classmethod
    def ECS_R6_3X(cls):
        return AliCloudInstanceType("ecs.r6.3xlarge", cores=12, memory=96)

    @classmethod
    def ECS_R6_4X(cls):
        return AliCloudInstanceType("ecs.r6.4xlarge", cores=16, memory=128)

    @classmethod
    def ECS_R6_6X(cls):
        return AliCloudInstanceType("ecs.r6.6xlarge", cores=24, memory=192)

    @classmethod
    def ECS_R6_8X(cls):
        return AliCloudInstanceType("ecs.r6.8xlarge", cores=32, memory=256)

    @classmethod
    def ECS_R6_13X(cls):
        return AliCloudInstanceType("ecs.r6.13xlarge", cores=52, memory=384)

    @classmethod
    def ECS_R6_26X(cls):
        return AliCloudInstanceType("ecs.r6.26xlarge", cores=104, memory=768)

    @classmethod
    def ECS_R6E_X(cls):
        return AliCloudInstanceType("ecs.r6e.xlarge", cores=4, memory=32)

    @classmethod
    def ECS_R6E_2X(cls):
        return AliCloudInstanceType("ecs.r6e.2xlarge", cores=8, memory=64)

    @classmethod
    def ECS_R6E_4X(cls):
        return AliCloudInstanceType("ecs.r6e.4xlarge", cores=16, memory=128)

    @classmethod
    def ECS_R6E_8X(cls):
        return AliCloudInstanceType("ecs.r6e.8xlarge", cores=32, memory=256)

    @classmethod
    def ECS_R6E_13X(cls):
        return AliCloudInstanceType("ecs.r6e.13xlarge", cores=52, memory=384)

    @classmethod
    def ECS_R7_X(cls):
        return AliCloudInstanceType("ecs.r7.xlarge", cores=4, memory=32)

    @classmethod
    def ECS_R7_2X(cls):
        return AliCloudInstanceType("ecs.r7.2xlarge", cores=8, memory=64)

    @classmethod
    def ECS_R7_3X(cls):
        return AliCloudInstanceType("ecs.r7.3xlarge", cores=12, memory=96)

    @classmethod
    def ECS_R7_4X(cls):
        return AliCloudInstanceType("ecs.r7.4xlarge", cores=16, memory=128)

    @classmethod
    def ECS_R7_6X(cls):
        return AliCloudInstanceType("ecs.r7.6xlarge", cores=24, memory=192)

    @classmethod
    def ECS_R7_8X(cls):
        return AliCloudInstanceType("ecs.r7.8xlarge", cores=32, memory=256)

    @classmethod
    def ECS_R7_16X(cls):
        return AliCloudInstanceType("ecs.r7.16xlarge", cores=64, memory=512)

    # High Clock Speed
    @classmethod
    def ECS_HFC6_2X(cls):
        return AliCloudInstanceType("ecs.hfc6.2xlarge", cores=8, memory=16)

    @classmethod
    def ECS_HFC6_3X(cls):
        return AliCloudInstanceType("ecs.hfc6.3xlarge", cores=12, memory=24)

    @classmethod
    def ECS_HFC6_4X(cls):
        return AliCloudInstanceType("ecs.hfc6.4xlarge", cores=16, memory=32)

    @classmethod
    def ECS_HFC6_6X(cls):
        return AliCloudInstanceType("ecs.hfc6.6xlarge", cores=24, memory=48)

    @classmethod
    def ECS_HFC6_8X(cls):
        return AliCloudInstanceType("ecs.hfc6.8xlarge", cores=32, memory=64)

    @classmethod
    def ECS_HFC6_10X(cls):
        return AliCloudInstanceType("ecs.hfc6.10xlarge", cores=40, memory=96)

    @classmethod
    def ECS_HFC6_16X(cls):
        return AliCloudInstanceType("ecs.hfc6.16xlarge", cores=64, memory=128)

    @classmethod
    def ECS_HFG6_X(cls):
        return AliCloudInstanceType("cs.hfg6.xlarge", cores=4, memory=16)

    @classmethod
    def ECS_HFG6_2X(cls):
        return AliCloudInstanceType("cs.hfg6.2xlarge", cores=8, memory=32)

    @classmethod
    def ECS_HFG6_3X(cls):
        return AliCloudInstanceType("cs.hfg6.3xlarge", cores=12, memory=48)

    @classmethod
    def ECS_HFG6_4X(cls):
        return AliCloudInstanceType("cs.hfg6.4xlarge", cores=16, memory=64)

    @classmethod
    def ECS_HFG6_6X(cls):
        return AliCloudInstanceType("cs.hfg6.6xlarge", cores=24, memory=96)

    @classmethod
    def ECS_HFG6_8X(cls):
        return AliCloudInstanceType("cs.hfg6.8xlarge", cores=32, memory=128)

    @classmethod
    def ECS_HFG6_10X(cls):
        return AliCloudInstanceType("cs.hfg6.10xlarge", cores=40, memory=192)

    @classmethod
    def ECS_HFG6_16X(cls):
        return AliCloudInstanceType("cs.hfg6.16xlarge", cores=64, memory=256)

    @classmethod
    def ECS_HFG6_20X(cls):
        return AliCloudInstanceType("cs.hfg6.20xlarge", cores=80, memory=384)

    @classmethod
    def ECS_HFR6_X(cls):
        return AliCloudInstanceType("cs.hfr6.xlarge", cores=4, memory=32)

    @classmethod
    def ECS_HFR6_2X(cls):
        return AliCloudInstanceType("cs.hfr6.2xlarge", cores=8, memory=64)

    @classmethod
    def ECS_HFR6_3X(cls):
        return AliCloudInstanceType("cs.hfr6.3xlarge", cores=12, memory=96)

    @classmethod
    def ECS_HFR6_4X(cls):
        return AliCloudInstanceType("cs.hfr6.4xlarge", cores=16, memory=128)

    @classmethod
    def ECS_HFR6_6X(cls):
        return AliCloudInstanceType("cs.hfr6.6xlarge", cores=24, memory=192)

    @classmethod
    def ECS_HFR6_8X(cls):
        return AliCloudInstanceType("cs.hfr6.8xlarge", cores=32, memory=256)

    @classmethod
    def ECS_HFR6_10X(cls):
        return AliCloudInstanceType("cs.hfr6.10xlarge", cores=40, memory=384)

    @classmethod
    def ECS_HFR6_16X(cls):
        return AliCloudInstanceType("cs.hfr6.16xlarge", cores=64, memory=512)
