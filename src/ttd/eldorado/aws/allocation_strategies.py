from enum import Enum
from typing import Dict, Any


class OnDemandStrategy(Enum):
    """
    Allocation strategies for On-Demand instance fleets.
    See: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html#emr-instance-fleet-allocation-strategy
    """
    Prioritized = 'prioritized'
    LowestPrice = 'lowest-price'


class SpotStrategy(Enum):
    """
    Allocation strategies for Spot instance fleets.
    See: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html#emr-instance-fleet-allocation-strategy
    """
    PriceCapacityOptimised = 'price-capacity-optimized'
    CapacityOptimised = 'capacity-optimized'
    CapacityOptimisedPrioritized = 'capacity-optimized-prioritized'
    Diversified = 'diversified'
    LowestPrice = 'lowest-price'


class AllocationStrategyConfiguration:
    """"
    Configure the allocation strategy for an instance fleet.
    """

    def __init__(
        self, on_demand: OnDemandStrategy = OnDemandStrategy.LowestPrice, spot: SpotStrategy = SpotStrategy.PriceCapacityOptimised
    ):
        self._on_demand = on_demand
        self._spot = spot

    def as_spec(self, spot_weighted_capacity: int, use_on_demand_on_timeout: bool, timeout_in_minutes: int):
        launch_spec: Dict[str, Any] = {"OnDemandSpecification": {"AllocationStrategy": self._on_demand.value}}

        if use_on_demand_on_timeout and spot_weighted_capacity > 0:
            launch_spec["SpotSpecification"] = {
                "TimeoutDurationMinutes": timeout_in_minutes,
                "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                "AllocationStrategy": self._spot.value,
            }

        return launch_spec
