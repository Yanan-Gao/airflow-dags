from enum import Enum
from typing import List

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r6id import R6id
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd


class EmrInstanceClasses(Enum):
    GeneralPurpose = 'm'
    ComputeOptimized = 'c'
    MemoryOptimized = 'r'  # also 'x' and 'z'
    StorageOptimized = 'd'  # also 'i'
    NetworkOptimized = 'i'


class EmrInstanceSizes(Enum):
    OneX = 1
    TwoX = 2
    FourX = 4
    EightX = 8
    TwelveX = 12
    SixteenX = 16


def getFleetInstances(
    instance_class: EmrInstanceClasses,
    instance_size: EmrInstanceSizes,
    instance_capacity: int,
    on_demand: bool = True,
    ebs_size: int = None
) -> EmrFleetInstanceTypes:
    """
    Get EMR fleet instances configuration. Define the class, size of instances in a fleet.
    (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-supported-instance-types.html)
    NOTE: This function 1. does not mix up different instance classes and/or sizes in one fleet. All instances will have
                           weighted capacity of 1 and thus @param instance_capacity = number of instances in the fleet
                        2: does not provide EBS to any instance with local SSD storage (e.g. m6gd, r6gd).

    @param instance_class: Class of instances, provides us different instance purposes.
    @param instance_size: Size of instances, provides us different vCPU, and size of memory and storage types.
    @param instance_capacity: Total number of instances.
    @param on_demand: Use on_demand instance or spot instance.
    @param ebs_size: Amount of EBS storage to be attached to each instance.
    """
    error: List[str] = []
    if instance_class is None:
        error += "Instance class can't be None. "
    if instance_size is None:
        error += "Instance size can't be None. "
    if ebs_size is not None and ebs_size <= 0:
        error += f"Invalid EBS storage size {ebs_size}. "
    if instance_capacity < 1:
        error += f"Invalid instance capacity size {instance_capacity}. "
    if error:
        raise Exception(''.join(error))

    instance_types = []

    if instance_class == EmrInstanceClasses.GeneralPurpose:
        if instance_size == EmrInstanceSizes.OneX:
            instance_types = [M7g.m7gd_xlarge()]
        if instance_size == EmrInstanceSizes.TwoX:
            instance_types = [M7g.m7gd_2xlarge()]
        if instance_size == EmrInstanceSizes.FourX:
            instance_types = [M7g.m7gd_4xlarge()]
        if instance_size == EmrInstanceSizes.EightX:
            instance_types = [M7g.m7gd_8xlarge()]
        if instance_size == EmrInstanceSizes.TwelveX:
            instance_types = [M7g.m7gd_12xlarge()]
        if instance_size == EmrInstanceSizes.SixteenX:
            instance_types = [M7g.m7gd_16xlarge()]

    elif instance_class == EmrInstanceClasses.MemoryOptimized:
        if instance_size == EmrInstanceSizes.OneX:
            instance_types = [R7gd.r7gd_xlarge()]
        if instance_size == EmrInstanceSizes.TwoX:
            instance_types = [R7gd.r7gd_2xlarge()]
        if instance_size == EmrInstanceSizes.FourX:
            instance_types = [R7gd.r7gd_4xlarge()]
        if instance_size == EmrInstanceSizes.EightX:
            instance_types = [R7gd.r7gd_8xlarge()]
        if instance_size == EmrInstanceSizes.TwelveX:
            instance_types = [R7gd.r7gd_12xlarge(), R6gd.r6gd_12xlarge()]
        if instance_size == EmrInstanceSizes.SixteenX:
            instance_types = [R7gd.r7gd_16xlarge(), R6gd.r6gd_16xlarge()]

    elif instance_class == EmrInstanceClasses.NetworkOptimized:
        if instance_size == EmrInstanceSizes.OneX:
            instance_types = [R6id.r6id_xlarge()]
        if instance_size == EmrInstanceSizes.TwoX:
            instance_types = [R6id.r6id_2xlarge()]
        if instance_size == EmrInstanceSizes.FourX:
            instance_types = [R6id.r6id_4xlarge()]
        if instance_size == EmrInstanceSizes.EightX:
            instance_types = [R6id.r6id_8xlarge()]
        if instance_size == EmrInstanceSizes.TwelveX:
            instance_types = [R6id.r6id_12xlarge()]
        if instance_size == EmrInstanceSizes.SixteenX:
            instance_types = [R6id.r6id_16xlarge()]

    else:
        raise NotImplementedError

    if not instance_types:
        raise Exception("None instance types!")

    return EmrFleetInstanceTypes(
        instance_types=[instance_type.with_max_ondemand_price().with_fleet_weighted_capacity(1) for instance_type in instance_types],
        on_demand_weighted_capacity=instance_capacity
    )


def getMasterFleetInstances(
    instance_class: EmrInstanceClasses, instance_size: EmrInstanceSizes, ebs_size: int = None
) -> EmrFleetInstanceTypes:
    return getFleetInstances(instance_class, instance_size, 1, True, ebs_size)
