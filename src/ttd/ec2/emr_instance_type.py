from typing import Optional, Dict, Any, List

from ttd.ec2.aws_instance_bandwidth import AwsInstanceBandwidth
from ttd.ec2.aws_instance_storage import SsdAwsInstanceStorage, AwsInstanceStorage, EbsAwsInstanceStorage
from ttd.ec2.cluster_params import Defaults, ClusterParams, calc_cluster_params
from ttd.emr_version import EmrVersion


class EmrInstanceType:
    ON_DEMAND = 'ON_DEMAND'
    SPOT = 'SPOT'

    def __init__(
        self,
        instance_name: str,
        cores: int,
        memory: float,
        declared_memory: Optional[int] = None,
        instance_storage: Optional[AwsInstanceStorage] = None,
        network_bandwidth: Optional[AwsInstanceBandwidth] = None,
        ebs_bandwidth: Optional[AwsInstanceBandwidth] = None,
        minimum_emr_versions: Optional[List[EmrVersion]] = None
    ):
        self.declared_memory = declared_memory
        self.disk_bandwidth = ebs_bandwidth
        self.network_bandwidth = network_bandwidth
        self.instance_storage = instance_storage
        self.memory = memory
        self.cores = cores
        self.instance_name = instance_name
        self.bid_price = None  # In dollars
        self.ebs_size_gb: Optional[int] = None
        self.ebs_iops: Optional[int] = None
        self.ebs_throughput: Optional[int] = None
        self.market_type = self.ON_DEMAND
        self.weighted_capacity: int = cores  # fleet instance weight
        self.priority: Optional[int] = None
        self.minimum_emr_versions: Optional[List[EmrVersion]] = minimum_emr_versions

        self.use_default_cluster_params = True
        self.unsupported_cluster_calc = False

    def get_total_storage(self) -> float:
        match self.instance_storage:
            case SsdAwsInstanceStorage() as storage:
                return sum((volume.size for volume in storage.volumes))
            case EbsAwsInstanceStorage():
                return 0
            case _:
                return 0

    def with_bid_price(self, bid_price) -> 'EmrInstanceType':
        self.bid_price = bid_price
        self.market_type = self.SPOT
        return self

    def with_max_ondemand_price(self) -> 'EmrInstanceType':
        self.market_type = self.SPOT
        return self

    def with_ebs_size_gb(self, ebs_size_gb: int) -> 'EmrInstanceType':
        self.ebs_size_gb = ebs_size_gb
        return self

    def with_ebs_iops(self, ebs_iops: int) -> 'EmrInstanceType':
        self.ebs_iops = ebs_iops
        return self

    def with_ebs_throughput(self, ebs_throughput: int) -> 'EmrInstanceType':
        self.ebs_throughput = ebs_throughput
        return self

    def with_fleet_weighted_capacity(self, weighted_capacity: int) -> 'EmrInstanceType':
        self.weighted_capacity = weighted_capacity
        return self

    def with_priority(self, priority: int) -> 'EmrInstanceType':
        """
        When used in conjunction with the prioritised allocation strategy, allows you to configure which instance types EMR
        will try and configure if available. The lower the number, the higher the priority.
        """
        self.priority = priority
        return self

    def with_cluster_calc_unsupported(self) -> 'EmrInstanceType':
        """
        Some instances can not have cluster calculations correctly done for them.
        In this case, we want to error in the gitlab pipeline before it has a chance to fail on EMR.

        @rtype: object
        """
        self.unsupported_cluster_calc = True
        return self

    def as_instance_spec(self) -> Dict[str, Any]:
        """
        Returns the configuration of this instance type as a dictionary to be used in API requests.
        """
        config = {
            "InstanceType": self.instance_name,
            "WeightedCapacity": self.weighted_capacity,
        }

        if self.ebs_size_gb is not None:
            config.update({
                "EbsConfiguration": {
                    "EbsOptimized":
                    True,
                    "EbsBlockDeviceConfigs": [{
                        "VolumeSpecification": {
                            "VolumeType": "gp3",
                            "SizeInGB": self.ebs_size_gb,
                            "Iops": self._calc_ebs_iops(),
                            "Throughput": self._calc_ebs_throughput(),
                        },
                        "VolumesPerInstance": 1,
                    }],
                }
            }, )

        if self.market_type is EmrInstanceType.ON_DEMAND or self.bid_price is None:
            config.update({"BidPriceAsPercentageOfOnDemandPrice": 100})
        else:
            config.update({"BidPrice": self.bid_price})

        if self.priority is not None:
            config.update({"Priority": self.priority})

        return config

    def instance_spec_defined(self) -> bool:
        """
        Returns true if `memory` and `core` props are defined for this instance type,
        which means that it's possible to call `calc_cluster_params` for this instance.

        :return: `True` if instance specification is defined, otherwise `False`.
        """
        return self.memory is not None and self.cores is not None

    def copy_config_options_from(self, other: 'EmrInstanceType') -> 'EmrInstanceType':
        self.bid_price = other.bid_price
        self.ebs_size_gb = other.ebs_size_gb
        self.ebs_iops = other.ebs_iops
        self.ebs_throughput = other.ebs_throughput
        self.market_type = other.market_type
        self.weighted_capacity = other.weighted_capacity
        self.priority = other.priority
        self.use_default_cluster_params = other.use_default_cluster_params
        self.unsupported_cluster_calc = other.unsupported_cluster_calc
        return self

    def calc_cluster_params(
        self,
        instances: int,
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
        :type parallelism_factor: int
        :param parallelism_factor: Scale factor of parallelism. Default is 2.
        :type min_executor_memory: Optional[int]
        :param min_executor_memory: Minimum amount of memory in GB each executor should have.
            Specifying this parameter will influence how many executors per node will be created by hadoop.
            For performance reason number of cores per executor will be limited to :max_cores_executor.
        :type max_cores_executor: int
        :param max_cores_executor: Limit number of cores per executor. Default is 5.
        :type memory_tolerance: float
        :param memory_tolerance: The ratio of memory that is available to YARN to create executors. It was noticed that in some weird cases
            on big machines there is loss of memory of small amount that prevents requested number of executors to fit onto machine.
            By reducing "available" memory by 0.15% we add tolerance for params calculation and for YARN.
        :type partitions: int
        :param partitions: Number of partitions to use. If not specified, parallelism will be used.
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
            partitions,
        )

    def _calc_ebs_iops(self) -> int:
        if self.ebs_iops is not None:
            return self.ebs_iops
        elif self.ebs_size_gb < Defaults.GP3_IOPS_CALC_THRESHOLD_GIB:
            return Defaults.GP3_IOPS
        else:
            return min(
                Defaults.MAX_GP3_IOPS,
                self.ebs_size_gb * Defaults.IOPS_PER_GIB_MULTIPLIER,
            )

    def _calc_ebs_throughput(self) -> int:
        if self.ebs_throughput is not None:
            return self.ebs_throughput
        elif self.ebs_size_gb < Defaults.GP3_THROUGHPUT_CALC_THRESHOLD_GIB:
            return Defaults.GP3_THROUGHPUT_MIB_PER_SEC
        else:
            return Defaults.MAX_GP2_THROUGHPUT_MIB_PER_SEC
