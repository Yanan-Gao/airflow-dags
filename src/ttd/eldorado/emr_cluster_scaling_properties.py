from typing import Dict, Optional


class EmrClusterScalingProperties:

    def __init__(
        self,
        maximum_capacity_units: int,
        minimum_capacity_units: int,
        maximum_core_capacity_units: Optional[int] = None,
        maximum_on_demand_capacity_units: Optional[int] = None,
    ):
        self.managed_scaling_config = {
            "ComputeLimits": {
                "MaximumCapacityUnits": maximum_capacity_units,
                "MinimumCapacityUnits": minimum_capacity_units,
                "UnitType": "InstanceFleetUnits",
            }
        }

        if maximum_capacity_units < minimum_capacity_units:
            raise Exception("Cluster scaling: max capacity cannot be lower than min capacity!")

        if maximum_core_capacity_units is not None:
            if maximum_core_capacity_units < minimum_capacity_units:
                raise Exception("Cluster scaling: max core capacity cannot be lower than min capacity!")
            else:
                self.managed_scaling_config["ComputeLimits"]["MaximumCoreCapacityUnits"] = maximum_core_capacity_units

        if maximum_on_demand_capacity_units is not None:
            if maximum_on_demand_capacity_units < minimum_capacity_units:
                raise Exception("Cluster scaling: max on demand capacity cannot be lower than min capacity!")
            else:
                self.managed_scaling_config["ComputeLimits"]["MaximumOnDemandCapacityUnits"] = maximum_on_demand_capacity_units

    def get_config(self) -> Dict[str, Dict[str, str]]:
        return self.managed_scaling_config
