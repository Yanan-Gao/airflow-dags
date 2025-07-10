from dataclasses import dataclass
from typing import Optional, Dict, Literal

from kubernetes.client import V1ResourceRequirements, V1Pod, V1PodSpec, V1Container
from ttd.kubernetes.ephemeral_volume_config import EphemeralVolumeConfig


@dataclass
class PodResources:
    """
    Stores resource requests and limits for a Kubernetes pod.

    Provides helper methods to convert to native K8s objects.
    """

    request_cpu: str
    request_memory: str
    limit_memory: str
    limit_cpu: Optional[str] = None
    request_ephemeral_storage: Optional[str] = None
    limit_ephemeral_storage: str = "1Gi"
    ephemeral_volume_config: Optional[EphemeralVolumeConfig] = None

    def as_k8s_object(self) -> V1ResourceRequirements:
        resource_requirements = V1ResourceRequirements(
            requests={
                "cpu": self.request_cpu,
                "memory": self.request_memory
            },
            limits={
                "memory": self.limit_memory,
                "ephemeral-storage": self.limit_ephemeral_storage,
            },
        )

        if self.limit_cpu is not None:
            resource_requirements.limits["cpu"] = self.limit_cpu

        if self.request_ephemeral_storage is not None:
            resource_requirements.requests["ephemeral-storage"] = self.request_ephemeral_storage

        return resource_requirements

    def as_executor_config(self) -> Dict[Literal["pod_override"], V1Pod]:
        pod_override = V1Pod(spec=V1PodSpec(containers=[V1Container(name="base", resources=self.as_k8s_object())], ))

        if self.ephemeral_volume_config is not None:
            pod_override.spec.volumes = [self.ephemeral_volume_config.create_volume()]
            pod_override.spec.containers[0].volume_mounts = [self.ephemeral_volume_config.create_volume_mount()]

        return {"pod_override": pod_override}

    @staticmethod
    def from_dict(
        request_cpu: str,
        request_memory: str,
        limit_memory: str,
        limit_cpu: Optional[str] = None,
        request_ephemeral_storage: Optional[str] = None,
        limit_ephemeral_storage: str = "1Gi",
        ephemeral_volume_config: Optional[Dict[str, str]] = None,
    ) -> "PodResources":
        return PodResources(
            request_cpu=request_cpu,
            request_memory=request_memory,
            limit_memory=limit_memory,
            limit_cpu=limit_cpu,
            request_ephemeral_storage=request_ephemeral_storage,
            limit_ephemeral_storage=limit_ephemeral_storage,
            ephemeral_volume_config=EphemeralVolumeConfig(**ephemeral_volume_config) if ephemeral_volume_config is not None else None,
        )
