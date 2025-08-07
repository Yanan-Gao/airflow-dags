from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ttdenv import TtdEnvFactory

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

# master/core instance types for all datasets
# Use larger instance for driver to handle 64GB memory + overhead comfortably
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

# Use r6gd.12xlarge for better memory/CPU ratio and network performance
# r6gd.12xlarge: 48 vCPUs, 384 GB RAM, 2 x 1425 GB NVMe, 25 Gbps network
core_instance_types = [
    R6gd.r6gd_12xlarge().with_fleet_weighted_capacity(12),
]

# Adjust weighted capacity for larger instances
on_demand_weighted_capacity = 24  # 2 x r6gd.12xlarge instances as minimum

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=core_instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity
)

# Scale up to ~20 r6gd.12xlarge instances max (240 weighted capacity)
# This provides up to 300 executors with 24GB each
scaling_policy = EmrClusterScalingProperties(
    maximum_capacity_units=on_demand_weighted_capacity * 10,  # 240 units = 20 instances
    minimum_capacity_units=on_demand_weighted_capacity,  # 24 units = 2 instances
    maximum_core_capacity_units=on_demand_weighted_capacity * 10,
    maximum_on_demand_capacity_units=on_demand_weighted_capacity * 10
)

# TODO now that we have introduced autoscaling this config can probably be removed since there is a bunch of copy paste
# configurations for each dataset. Cluster hardware for now. More dataset specific can be added.
DSR_DELETE_DATASET_CONFIG = {
    "AZURE": {},
    "AWS": {
        "iav2householdgraph": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "iav2graph": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "tapad_na": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "tapad_eur": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "tapad_apac": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "adgraph": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "bidrequest": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "identity": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "availshashedid": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        # "avails30": {
        #     "master_fleet_instance_type_configs": ElDoradoFleetInstanceTypes(
        #         instance_types=master_instance_types,
        #         on_demand_weighted_capacity=1
        #     ),
        #     "core_fleet_instance_type_configs": ElDoradoFleetInstanceTypes(
        #         instance_types=core_instance_types,
        #         spot_weighted_capacity=default_spot_weighted_capacity,
        #         on_demand_weighted_capacity=2*default_on_demand_weighted_capacity
        #     )
        # },
        # "avails7": {
        #     "master_fleet_instance_type_configs": ElDoradoFleetInstanceTypes(
        #         instance_types=master_instance_types,
        #         on_demand_weighted_capacity=default_spot_weighted_capacity
        #     ),
        #     "core_fleet_instance_type_configs": ElDoradoFleetInstanceTypes(
        #         instance_types=core_instance_types,
        #         spot_weighted_capacity=default_spot_weighted_capacity,
        #         on_demand_weighted_capacity=16*default_on_demand_weighted_capacity
        #     )
        # },
        "householdgraph": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "rtb_conversiontracker_cleanfile": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "rtb_conversiontracker_verticaload": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "rtb_bidfeedback_cleanfile": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        "attributedevent": {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
    }
}

PARTNER_DSR_DATASET_CONFIG = {
    # 'AZURE': {
    #     'rtb_clicktracker_cleanfile': {
    #         'task_name': 'clicktracker',
    #         'HDIVMConfig_override': {
    #             'num_workernode': 4
    #         }
    #     },
    #     'rtb_videoevent_cleanfile': {
    #         'task_name': 'videoevent',
    #         'HDIVMConfig_override': {
    #             'num_workernode': 4
    #         }
    #     },
    # },
    'AWS': {
        'rtb_clicktracker_cleanfile': {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        'rtb_videoevent_cleanfile': {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        'rtb_conversiontracker_cleanfile': {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        },
        'rtb_conversiontracker_verticaload': {
            "master_fleet_instance_type_configs": master_fleet_instance_type_configs,
            "core_fleet_instance_type_configs": core_fleet_instance_type_configs,
            "managed_cluster_scaling_config": scaling_policy
        }
    },
}
