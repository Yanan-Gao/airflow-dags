from dags.forecast.sketches.randomly_sampled_avails.constants import STANDARD_CORE_FLEET_INSTANCE_TYPE_CONFIGS
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes


def get_core_fleet_instance_type_configs(on_demand_capacity: int, instance_types=STANDARD_CORE_FLEET_INSTANCE_TYPE_CONFIGS):
    return EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=on_demand_capacity)


TEST_MODE = False


def get_test_or_default_value(test_value, default):
    return test_value if TEST_MODE else default
