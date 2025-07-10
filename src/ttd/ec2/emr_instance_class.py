from typing import List, Optional

from ttd.ec2.emr_instance_type import EmrInstanceType


class EmrInstanceClass:

    def __init__(self, instance_types: List[EmrInstanceType], fallback_instance_types: Optional[List[EmrInstanceType]] = None):
        self._instance_types: List[EmrInstanceType] = instance_types
        self._fallback_instance_types = fallback_instance_types

        self._with_fallback_instance = False

        self._ebs_throughput: Optional[int] = None
        self._ebs_iops: Optional[int] = None
        self._ebs_size_gb: Optional[int] = None
        self._weighted_capacity: Optional[int] = None

    def with_weighted_capacity(self, weighted_capacity) -> 'EmrInstanceClass':
        self._weighted_capacity = weighted_capacity
        return self

    def with_ebs_size_gb(self, ebs_size_gb: int) -> 'EmrInstanceClass':
        self._ebs_size_gb = ebs_size_gb
        return self

    def with_ebs_iops(self, ebs_iops: int) -> 'EmrInstanceClass':
        self._ebs_iops = ebs_iops
        return self

    def with_ebs_throughput(self, ebs_throughput: int) -> 'EmrInstanceClass':
        self._ebs_throughput = ebs_throughput
        return self

    def with_fallback_instance(self) -> 'EmrInstanceClass':
        if self._fallback_instance_types is None:
            return self
        self._with_fallback_instance = True
        return self

    def get_instance_types(self) -> List[EmrInstanceType]:
        """
        Return list of EmrInstanceType instances updated with relevant properties.
        """
        instance_types: List[EmrInstanceType] = self._instance_types
        if self._with_fallback_instance and self._fallback_instance_types is not None:
            instance_types.extend(self._fallback_instance_types)

        for i in range(0, len(instance_types)):
            it = instance_types[i]
            it.priority = i + 1
            if self._ebs_throughput is not None:
                it.with_ebs_size_gb(self._ebs_size_gb)
            if self._ebs_iops is not None:
                it.with_ebs_iops(self._ebs_iops)
            if self._ebs_throughput is not None:
                it.with_ebs_throughput(self._ebs_throughput)
            if self._weighted_capacity is not None:
                it.with_fleet_weighted_capacity(self._weighted_capacity)

        return instance_types
