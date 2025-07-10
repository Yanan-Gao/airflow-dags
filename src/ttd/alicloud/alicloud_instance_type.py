from typing import Optional

from ttd.ec2.cluster_params import Defaults, ClusterParams, calc_cluster_params


class AliCloudInstanceType:

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

        :param instances: Number of instances of worker nodes.
        :param parallelism_factor: Scale factor of parallelism. Default is 2.
        :param min_executor_memory: Minimum amount of memory in GB each executor should have.
            Specifying this parameter will influence how many executors per node will be created by hadoop.
            For performance reason number of cores per executor will be limited to :max_cores_executor.
        :param max_cores_executor: Limit number of cores per executor. Default is 5.
        :param memory_tolerance: The ratio of memory that is available to YARN to create executors. It was noticed that in some weird cases
            on big machines there is loss of memory of small amount that prevents requested number of executors to fit onto machine.
            By reducing "available" memory by 0.15% we add tolerance for params calculation and for YARN.
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
