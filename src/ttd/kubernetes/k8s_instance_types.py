class K8sInstanceType:

    def __init__(self, instance_name: str, cloud_provider: str, cores: int, memory: float):
        self.instance_name = instance_name
        self.cloud_provider = cloud_provider
        self.cores = cores
        self.memory = memory


class K8sInstanceTypes:
    """
    Instance types available through K8s.
    """

    # TODO - not sure what instance types are available through Azure.
    @classmethod
    def aws_compute_optimised(cls) -> list[K8sInstanceType]:
        return [cls.c6a_2xlarge(), cls.c6a_4xlarge(), cls.c6a_8xlarge(), cls.c6a_12xlarge(), cls.c6a_16xlarge()]

    @classmethod
    def aws_general_purpose(cls) -> list[K8sInstanceType]:
        return [
            cls.m6a_2xlarge(),
            cls.m6a_4xlarge(),
            cls.m6a_8xlarge(),
            cls.m6a_12xlarge(),
            cls.m6a_16xlarge(),
            cls.m6a_24xlarge(),
            cls.m6a_48xlarge()
        ]

    @classmethod
    def aws_memory_optimised(cls) -> list[K8sInstanceType]:
        return [
            cls.r6a_2xlarge(),
            cls.r6a_4xlarge(),
            cls.r6a_8xlarge(),
            cls.r6a_16xlarge(),
            cls.r6a_24xlarge(),
            cls.r6a_32xlarge(),
            cls.r6a_48xlarge()
        ]

    @classmethod
    def c6a_2xlarge(cls):
        return K8sInstanceType("c6a.2xlarge", "aws", 8, 16)

    @classmethod
    def c6a_4xlarge(cls):
        return K8sInstanceType("c6a.4xlarge", "aws", 16, 32)

    @classmethod
    def c6a_8xlarge(cls):
        return K8sInstanceType("c6a.8xlarge", "aws", 32, 64)

    @classmethod
    def c6a_12xlarge(cls):
        return K8sInstanceType("c6a.12xlarge", "aws", 48, 96)

    @classmethod
    def c6a_16xlarge(cls):
        return K8sInstanceType("c6a.16xlarge", "aws", 64, 128)

    @classmethod
    def m6a_2xlarge(cls):
        return K8sInstanceType("m6a.2xlarge", "aws", 8, 32)

    @classmethod
    def m6a_4xlarge(cls):
        return K8sInstanceType("m6a.4xlarge", "aws", 16, 64)

    @classmethod
    def m6a_8xlarge(cls):
        return K8sInstanceType("m6a.8xlarge", "aws", 32, 128)

    @classmethod
    def m6a_12xlarge(cls):
        return K8sInstanceType("m6a.12xlarge", "aws", 48, 192)

    @classmethod
    def m6a_16xlarge(cls):
        return K8sInstanceType("m6a.16xlarge", "aws", 64, 256)

    @classmethod
    def m6a_24xlarge(cls):
        return K8sInstanceType("m6a.24xlarge", "aws", 96, 384)

    @classmethod
    def m6a_48xlarge(cls):
        return K8sInstanceType("m6a.48xlarge", "aws", 192, 768)

    @classmethod
    def m7a_2xlarge(cls):
        return K8sInstanceType("m7a.2xlarge", "aws", 8, 32)

    @classmethod
    def r6a_2xlarge(cls):
        return K8sInstanceType("r6a.2xlarge", "aws", 8, 64)

    @classmethod
    def r6a_4xlarge(cls):
        return K8sInstanceType("r6a.4xlarge", "aws", 16, 128)

    @classmethod
    def r6a_8xlarge(cls):
        return K8sInstanceType("r6a.8xlarge", "aws", 32, 256)

    @classmethod
    def r6a_16xlarge(cls):
        return K8sInstanceType("r6a.16xlarge", "aws", 64, 512)

    @classmethod
    def r6a_24xlarge(cls):
        return K8sInstanceType("r6a.24xlarge", "aws", 96, 768)

    @classmethod
    def r6a_32xlarge(cls):
        return K8sInstanceType("r6a.32xlarge", "aws", 128, 1024)

    @classmethod
    def r6a_48xlarge(cls):
        return K8sInstanceType("r6a.48xlarge", "aws", 192, 1536)

    @classmethod
    def ecs_g5ne_2xlarge(cls):
        return K8sInstanceType("ecs.g5ne.2xlarge", "alicloud", 8, 32)

    @classmethod
    def ecs_g6_4xlarge(cls):
        return K8sInstanceType("ecs.g6.4xlarge", "alicloud", 16, 64)
