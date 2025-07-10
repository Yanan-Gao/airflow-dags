from typing import Dict, Literal

from kubernetes.client import V1Pod

from ttd.kubernetes.pod_resources import PodResources


class K8sExecutorConfig:
    """ "
    Provides default executor config values for tasks using Kubernetes executors.
    """

    @staticmethod
    def watch_task() -> Dict[Literal["pod_override"], V1Pod]:
        pod_resources = PodResources(
            request_cpu="100m",
            request_memory="750Mi",
            limit_memory="937Mi",
            limit_ephemeral_storage="200Mi",
        )

        return pod_resources.as_executor_config()

    @staticmethod
    def light_data_transfer_task() -> Dict[Literal["pod_override"], V1Pod]:
        pod_resources = PodResources(
            request_cpu="500m",
            request_memory="1Gi",
            limit_memory="2Gi",
            limit_ephemeral_storage="1Gi",
        )

        return pod_resources.as_executor_config()
