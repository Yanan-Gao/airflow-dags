from typing import List, Dict, Optional, Any

from math import floor

from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.eldorado.aws.allocation_strategies import AllocationStrategyConfiguration, OnDemandStrategy
from ttd.emr_version import EmrVersion


class EmrFleetInstanceTypes:
    TYPES_LIMIT_FOR_NODE_GROUP = {
        "master": 5,
        "core": 15,
        "task": 15,
    }

    def __init__(
        self,
        instance_types: Optional[List[EmrInstanceType]] = None,
        on_demand_weighted_capacity: int = 0,
        allocation_strategy: AllocationStrategyConfiguration = AllocationStrategyConfiguration(),
        spot_weighted_capacity: int = 0,
        node_group: str = "core",
        instance_classes: Optional[List[EmrInstanceClass]] = None,
    ):
        """
        El-Dorado Fleet Instance Cluster
        Capacity is based on weights assigned to EmrInstanceType. EmrInstanceType cores are used by default.

        :param instance_types: List of EmrInstanceTypes to be used, max of 5
        :param on_demand_weighted_capacity: Requested weighted on demand capacity
        :param allocation_strategy: Instance fleet allocation strategy. See AllocationStrategyConfiguration
        :param spot_weighted_capacity: Requested weighted spot capacity
        """

        if instance_types is None and instance_classes is None or instance_types is not None and instance_classes is not None:
            raise ValueError("EmrFleetInstanceTypes requires either instance_types or instance_classes parameters specified")

        if instance_types is not None:
            self.instance_types = instance_types
        else:
            self.instance_types = [i for ic in instance_classes for i in ic.get_instance_types()]
            allocation_strategy = AllocationStrategyConfiguration(OnDemandStrategy.Prioritized, allocation_strategy._spot)

        node_groups = self.TYPES_LIMIT_FOR_NODE_GROUP.keys()
        if node_group.lower() not in node_groups:
            raise Exception(f"The node group type must be one of: ({', '.join(node_groups)}), node group type present: {node_group}")

        ## There is no limit per node group defined in AWS EMR
        # node_group_limit = self.TYPES_LIMIT_FOR_NODE_GROUP.get(node_group)
        #
        # if node_group.lower() in node_groups and len(self.instance_types) > node_group_limit:
        #     raise Exception(
        #         f"Max of {node_group_limit} instance types supported for {node_group}, instance types specified: {len(self.instance_types)}",
        #     )

        if (on_demand_weighted_capacity + spot_weighted_capacity) < 1:
            raise Exception("Minimal capacity for fleet instance is 1 or greater")

        if len(self.instance_types) > 30:
            raise Exception("You cannot specify more than 30 instance types in your fleet")

        self.on_demand_weighted_capacity = on_demand_weighted_capacity
        self.allocation_strategy = allocation_strategy
        self.spot_weighted_capacity = spot_weighted_capacity
        self.node_group = node_group

    def get_base_instance(self) -> EmrInstanceType:
        """
        Get the lowest weighted instance type.
        This can be used to create configurations that scale to other instance types.

        @return: Lowest weighted EmrInstanceType
        """
        return min(self.instance_types, key=lambda x: x.weighted_capacity)

    def get_scaled_instance_count(self) -> int:
        """
        Calculates instance count based on the lowest weighted instance type
        """
        return int(floor((self.on_demand_weighted_capacity + self.spot_weighted_capacity) / self.get_base_instance().weighted_capacity))

    def get_scaled_total_cores(self) -> int:
        """
        Estimates requested cores based on the base instance core count and the equivalent number of these instances.
        """
        return self.get_base_instance().cores * self.get_scaled_instance_count()

    def get_scaled_total_memory(self) -> float:
        """
        Estimates requested memory based on the base instance memory and the equivalent number of these instances.
        """
        return self.get_base_instance().memory * self.get_scaled_instance_count()

    def get_fleet_instance_type_configs(self, emr_version: EmrVersion) -> List[Dict[str, Any]]:
        """
        Returns EMR Fleet Instance Type configs to create Fleet Instance Cluster in AWS EMR
        """
        fleet_config = [
            instance_type.as_instance_spec() for instance_type in emr_version.filter_by_minimum_emr_versions(self.instance_types)
        ]

        return fleet_config

    def get_launch_spec(self, use_on_demand_on_timeout: bool, timeout_in_minutes: int) -> Dict:
        return self.allocation_strategy.as_spec(self.spot_weighted_capacity, use_on_demand_on_timeout, timeout_in_minutes)

    @classmethod
    def copy_with_different_instances(
        cls, other: 'EmrFleetInstanceTypes', instance_types: list[EmrInstanceType]
    ) -> 'EmrFleetInstanceTypes':
        return cls(
            instance_types=instance_types,
            on_demand_weighted_capacity=other.on_demand_weighted_capacity,
            allocation_strategy=other.allocation_strategy,
            spot_weighted_capacity=other.spot_weighted_capacity,
            node_group=other.node_group,
        )
