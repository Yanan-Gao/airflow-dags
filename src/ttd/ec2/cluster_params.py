from math import floor
from typing import Optional, Dict


class ClusterParamsException(Exception):
    pass


class ClusterParams:

    def __init__(
        self,
        instances: int,
        vcores: int,
        node_memory: int,
        executor_instances: int,
        executor_cores: int,
        parallelism: int,
        partitions: int,
        executor_memory: int,
        executor_memory_overhead: int,
        executor_memory_unit: str = "m",
    ):
        if parallelism <= 0:
            raise ClusterParamsException(f"Parallelism cannot be <= 0, found: {parallelism}")

        if partitions <= 0:
            raise ClusterParamsException(f"Partitions cannot be <= 0, found: {partitions}")

        self._instances = instances
        self._node_memory = node_memory
        self._vcores = vcores
        self._executor_memory_overhead = executor_memory_overhead
        self._executor_memory = executor_memory
        self._parallelism = parallelism
        self._partitions = partitions
        self._executor_cores = executor_cores
        self._executor_instances = executor_instances
        self._executor_memory_unit = executor_memory_unit

    @property
    def instances(self) -> int:
        return self._instances

    @property
    def node_memory(self) -> int:
        return self._node_memory

    @property
    def vcores(self) -> int:
        """
        Number of cores each worker node has
        """
        return self._vcores

    @property
    def executor_memory_overhead(self) -> int:
        return self._executor_memory_overhead

    @property
    def executor_memory_overhead_with_unit(self) -> str:
        return self._executor_memory_overhead.__str__() + self._executor_memory_unit

    @property
    def executor_memory(self) -> int:
        return self._executor_memory

    @property
    def executor_memory_with_unit(self) -> str:
        return self._executor_memory.__str__() + self._executor_memory_unit

    @property
    def parallelism(self) -> int:
        return self._parallelism

    @property
    def executor_cores(self) -> int:
        return self._executor_cores

    @property
    def executor_instances(self) -> int:
        return self._executor_instances

    @property
    def executor_memory_unit(self) -> str:
        return self._executor_memory_unit

    @property
    def partitions(self) -> int:
        return self._partitions

    def to_spark_arguments(self) -> Dict[str, Dict[str, str | int]]:
        """
        Returns Spark Resources configuration arguments as they used for sparkSubmit command.
        For details see docs of `calc_cluster_params`

        :return: Dictionary of arguments for spark-submit command.

        Structure of the dictionary:

        {
            "args": {
                "executor-memory": 123,

                # other arguments
            },

            "conf_args": {
                "spark.executor.instances": 10,

                # other "--conf a=b" arguments
            }
        }
        """
        return {
            "args": {
                "executor-memory": self.executor_memory_with_unit,
                "executor-cores": self.executor_cores,
            },
            "conf_args": {
                "spark.executor.instances": self.executor_instances,
                "spark.executor.cores": self.executor_cores,
                "spark.executor.memory": f"{self.executor_memory_with_unit}",
                "spark.executor.memoryOverhead": f"{self.executor_memory_overhead_with_unit}",
                "yarn.nodemanager.resource.memory-mb": f"{self.node_memory * 1000}",
                "yarn.nodemanager.resource.cpu-vcores": self.vcores,
                "spark.driver.memory": f"{self.executor_memory_with_unit}",
                "spark.driver.cores": self.executor_cores,
                "spark.driver.memoryOverhead": f"{self.executor_memory_overhead_with_unit}",
                "spark.default.parallelism": self.parallelism,
                "spark.sql.shuffle.partitions": self.partitions,
                # Usual Spark Cluster Defaults
                "spark.speculation": "false",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.executor.extraJavaOptions": "-server -XX:+UseParallelGC",
                "spark.sql.files.ignoreCorruptFiles": "false",
            },
        }


class Defaults:
    __slots__ = ()
    PARALLELISM_FACTOR: int = 2
    MAX_CORES_EXECUTOR: int = 5
    MEMORY_TOLERANCE: float = 0.9985

    GP3_IOPS: int = 3000
    MAX_GP3_IOPS: int = 16000
    GP3_IOPS_CALC_THRESHOLD_GIB: int = 1000
    IOPS_PER_GIB_MULTIPLIER: int = 3

    GP3_THROUGHPUT_MIB_PER_SEC: int = 125
    MAX_GP2_THROUGHPUT_MIB_PER_SEC: int = 250
    MAX_GP3_THROUGHPUT_MIB_PER_SEC: int = 1000
    GP3_THROUGHPUT_CALC_THRESHOLD_GIB: int = 170


class ClusterCalcDefaults:
    """
    Holds default configuration for cluster parameters calculation method. Is used in context of `eldorado.py` functionality to allow
    automatic parameter calculation in `ElDoradoStep`

    :param parallelism_factor: Scale factor of parallelism. Default is 2.
    :param min_executor_memory: Minimum amount of memory in GB each executor should have.
        Specifying this parameter will influence how many executors per node will be created by hadoop.
        For performance reason number of cores per executor will be limited to :max_cores_executor.
    :param max_cores_executor: Limit number of cores per executor. Default is 5.
    :param memory_tolerance: The ratio of memory that is available to YARN to create executors. It was noticed that in some weird cases
        on big machines there is loss of memory of small amount that prevents requested number of executors to fit onto machine.
        By reducing "available" memory by 0.15% we add tolerance for params calculation and for YARN.
    :param partitions: Number of partitions to use. If not specified, parallelism will be used.
    """

    def __init__(
        self,
        parallelism_factor: int = Defaults.PARALLELISM_FACTOR,
        min_executor_memory: Optional[int] = None,
        max_cores_executor: int = Defaults.MAX_CORES_EXECUTOR,
        memory_tolerance: float = Defaults.MEMORY_TOLERANCE,
        partitions: Optional[int] = None,
    ):
        self.parallelism_factor = parallelism_factor
        self.min_executor_memory = min_executor_memory
        self.max_cores_executor = max_cores_executor
        self.memory_tolerance = memory_tolerance
        self.partitions = partitions


def calc_cluster_params(
    instances: int,
    vcores: int,
    memory: int,
    parallelism_factor: int = Defaults.PARALLELISM_FACTOR,
    min_executor_memory: Optional[int] = None,
    max_cores_executor: int = Defaults.MAX_CORES_EXECUTOR,
    memory_tolerance: float = Defaults.MEMORY_TOLERANCE,
    partitions: Optional[int] = None,
) -> ClusterParams:
    """
    Calculates parameters of the cluster (number of executors, cores per executor and memory) from provided general specs.

    :type instances: int
    :param instances: Number of instances of worker nodes.
    :type vcores: int
    :param vcores: Number of vCores of a worker instance in a cluster.
    :param memory: Memory (RAM) in GB of a worker instance in a cluster.
    :type parallelism_factor: int
    :param parallelism_factor: Scale factor of parallelism. Default is 2.
    :type min_executor_memory: Optional[int]
    :param min_executor_memory: Minimum amount of memory in GB each executor should have.
        Specifying this parameter will influence how many executors per node will be created by hadoop.
        For performance reason number of cores per executor will be limited to :max_cores_executor.
    :type max_cores_executor: int
    :param max_cores_executor: Limit number of cores per executor. Default is 5.
    :type memory_tolerance: int
    :param memory_tolerance: The ratio of memory that is available to YARN to create executors. It was noticed that in some weird cases
        on big machines there is loss of memory of small amount that prevents requested number of executors to fit onto machine.
        By reducing "available" memory by 0.15% we add tolerance for params calculation and for YARN.
    :type partitions: int
    :param partitions: Number of partitions to use. If not specified, parallelism will be used.
    :rtype: ClusterParams
    """

    def calc_executors_per_node(cores: int, core_per_executor: int) -> int:
        usable_cores = cores - 1
        executors = usable_cores // core_per_executor
        less_cores = usable_cores // (executors + 1)
        # Use less cores per executor if it allows to utilize more cores overall
        return (executors + 1 if less_cores * (executors + 1) > executors * core_per_executor else executors)

    def calc_executors_per_node_memory(node_memory: int, min_memory: int) -> int:
        return node_memory // min_memory

    # Each instance in EMR reserves 8GB of memory for OS and various daemons.
    # Some details are here: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html#emr-hadoop-task-config-r5
    # Also in some weird cases big cluster "looses" around 28MB causing fewer executors provisioned, in order to tolerate this lost
    # (and until we figure out where is the problem) decreasing amount of available memory by 0.99974
    available_node_memory = floor((memory - 8) * 1024 * memory_tolerance)

    executors_per_node: int = calc_executors_per_node(vcores, max_cores_executor)
    if (min_executor_memory is not None and available_node_memory // executors_per_node < min_executor_memory * 1024):
        executors_per_node = calc_executors_per_node_memory(available_node_memory, min_executor_memory * 1024)

    executor_instances: int = max(instances * executors_per_node - 1, 1)
    executor_cores: int = min((vcores - 1) // executors_per_node, max_cores_executor)
    parallelism: int = executor_instances * executor_cores * parallelism_factor
    if partitions is None:
        partitions = parallelism

    executor_memory_whole: int = available_node_memory // executors_per_node
    executor_memory_overhead: int = executor_memory_whole // 10  # take 10%
    executor_memory: int = executor_memory_whole - executor_memory_overhead

    return ClusterParams(
        instances,
        vcores,
        memory,
        executor_instances,
        executor_cores,
        parallelism,
        partitions,
        executor_memory,
        executor_memory_overhead,
        executor_memory_unit="m",
    )
