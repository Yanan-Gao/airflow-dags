# Add more instance types to this class as needed
from typing import Optional

from ttd.ec2.cluster_params import ClusterParams, calc_cluster_params, Defaults


class HDIInstanceTypes:
    # Dv2 series
    @classmethod
    def Standard_D3_v2(cls):
        return HDIInstanceType("STANDARD_D3_V2", cores=4, memory=14)

    @classmethod
    def Standard_D4_v2(cls):
        return HDIInstanceType("STANDARD_D4_V2", cores=8, memory=28)

    @classmethod
    def Standard_D5_v2(cls):
        return HDIInstanceType("STANDARD_D5_V2", cores=16, memory=56)

    @classmethod
    def Standard_D12_v2(cls):
        return HDIInstanceType("STANDARD_D12_V2", cores=4, memory=28)

    @classmethod
    def Standard_D13_v2(cls):
        return HDIInstanceType("STANDARD_D13_V2", cores=8, memory=56)

    @classmethod
    def Standard_D14_v2(cls):
        return HDIInstanceType("STANDARD_D14_V2", cores=16, memory=112)

    # Dv4 series
    @classmethod
    def Standard_D4A_v4(cls):
        return HDIInstanceType("STANDARD_D4A_V4", cores=4, memory=16)

    @classmethod
    def Standard_D8A_v4(cls):
        return HDIInstanceType("STANDARD_D8A_V4", cores=8, memory=32)

    @classmethod
    def Standard_D32A_v4(cls):
        return HDIInstanceType("STANDARD_D32A_V4", cores=32, memory=128)

    @classmethod
    def Standard_D96A_v4(cls):
        return HDIInstanceType("STANDARD_D96A_V4", cores=96, memory=384)

    # Dv5 series
    @classmethod
    def Standard_D8ads_v5(cls):
        return HDIInstanceType("STANDARD_D8ADS_V5", cores=8, memory=32)

    @classmethod
    def Standard_D16ads_v5(cls):
        return HDIInstanceType("STANDARD_D16ADS_V5", cores=16, memory=64)

    @classmethod
    def Standard_D32ads_v5(cls):
        return HDIInstanceType("STANDARD_D32ADS_V5", cores=32, memory=128)

    # A series
    @classmethod
    def Standard_A2_v2(cls):
        return HDIInstanceType("STANDARD_A2_V2", cores=2, memory=4)

    @classmethod
    def Standard_A4_v2(cls):
        return HDIInstanceType("STANDARD_A4_V2", cores=4, memory=8)

    @classmethod
    def Standard_A8_v2(cls):
        return HDIInstanceType("STANDARD_A8_V2", cores=8, memory=16)

    @classmethod
    def Standard_A2M_v2(cls):
        return HDIInstanceType("STANDARD_A2M_V2", cores=2, memory=16)

    @classmethod
    def Standard_A4M_v2(cls):
        return HDIInstanceType("STANDARD_A4M_V2", cores=4, memory=32)

    @classmethod
    def Standard_A8M_v2(cls):
        return HDIInstanceType("STANDARD_A8M_V2", cores=8, memory=64)

    # E series
    @classmethod
    def Standard_E4_v3(cls):
        return HDIInstanceType("STANDARD_E4_V3", cores=4, memory=32)

    @classmethod
    def Standard_E8_v3(cls):
        return HDIInstanceType("STANDARD_E8_V3", cores=8, memory=64)

    @classmethod
    def Standard_E16_v3(cls):
        return HDIInstanceType("STANDARD_E16_V3", cores=16, memory=128)

    @classmethod
    def Standard_E32_v3(cls):
        return HDIInstanceType("STANDARD_E32_V3", cores=32, memory=256)

    @classmethod
    def Standard_E64_v3(cls):
        return HDIInstanceType("STANDARD_E64_V3", cores=64, memory=432)

    # F series
    @classmethod
    def Standard_F8(cls):
        return HDIInstanceType("STANDARD_F8", cores=8, memory=16)

    _instances: dict[str, "HDIInstanceType"] = {}

    @classmethod
    def _initialize_instances(cls) -> None:
        if cls._instances:
            return

        for name, method in cls.__dict__.items():
            if isinstance(method, classmethod) and not name.startswith("_"):
                try:
                    instance_type = method.__get__(None, cls)()
                    if isinstance(instance_type, HDIInstanceType):
                        cls._instances[instance_type.instance_name.upper()] = instance_type
                except TypeError:
                    pass

    @classmethod
    def get_from_name(cls, name: str) -> Optional["HDIInstanceType"]:
        cls._initialize_instances()
        return cls._instances.get(name.upper(), None)


class HDIInstanceType:

    def __init__(self, instance_name: str, cores: int, memory: int):
        self.instance_name = instance_name
        self.cores = cores
        self.memory = memory

        self.unsupported_cluster_calc = False

    def with_cluster_calc_unsupported(self):
        """
        Some instances can not have cluster calculations correctly done for them.
        In this case, we want to error in the gitlab pipeline before it has a chance to fail on EMR.

        @rtype: object
        """
        self.unsupported_cluster_calc = True
        return self

    def instance_spec_defined(self) -> bool:
        """
        Returns true if `memory` and `core` props are defined for this instance type,
        which means that it's possible to call `calc_cluster_params` for this instance.

        :return: `True` if instance specification is defined, otherwise `False`.
        """
        return self.memory is not None and self.cores is not None

    def calc_cluster_params(
        self,
        instances: int,
        parallelism_factor: int = Defaults.PARALLELISM_FACTOR,
        min_executor_memory: Optional[int] = None,
        max_cores_executor: int = Defaults.MAX_CORES_EXECUTOR,
        memory_tolerance: float = Defaults.MEMORY_TOLERANCE,
    ) -> ClusterParams:
        """
        Calculates parameters of the cluster (number of executors, cores per executor and memory) from provided general specs.

        :type instances: int
        :param instances: Number of instances of worker nodes.
        :type parallelism_factor: int
        :param parallelism_factor: Scale factor of parallelism. Default is 2.
        :type min_executor_memory: Optional[int]
        :param min_executor_memory: Minimum amount of memory in GB each executor should have.
            Specifying this parameter will influence how many executors per node will be created by hadoop.
            For performance reason number of cores per executor will be limited to :max_cores_executor.
        :type max_cores_executor: int
        :param max_cores_executor: Limit number of cores per executor. Default is 5.
        :param memory_tolerance: The ratio of memory that is available to YARN to create executors. It was noticed that in some weird cases
            on big machines there is loss of memory of small amount that prevents requested number of executors to fit onto machine.
            By reducing "available" memory by 0.15% we add tolerance for params calculation and for YARN.
        :type memory_tolerance: float
        :rtype: ClusterParams
        """
        if self.unsupported_cluster_calc and instances == 1:
            raise Exception("Instance Type Not Supported")

        if not self.instance_spec_defined():
            raise Exception("Number of cores and memory for EMR Instance Type should be provided")

        return calc_cluster_params(
            instances,
            self.cores,
            self.memory,
            parallelism_factor,
            min_executor_memory,
            max_cores_executor,
            memory_tolerance,
        )
