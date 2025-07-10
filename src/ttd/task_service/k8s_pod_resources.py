from typing import Optional

from ttd.kubernetes.pod_resources import PodResources


class TaskServicePodResources:
    """
    Provides static methods to generate pod resources class
    """

    @staticmethod
    def small() -> PodResources:
        return PodResources(
            request_cpu="500m",
            request_memory="1Gi",
            limit_memory="2Gi",
            limit_ephemeral_storage="500Mi",
        )

    @staticmethod
    def medium() -> PodResources:
        return PodResources(
            request_cpu="1",
            request_memory="2Gi",
            limit_memory="4Gi",
            limit_ephemeral_storage="500Mi",
        )

    @staticmethod
    def large() -> PodResources:
        return PodResources(
            request_cpu="2",
            request_memory="4Gi",
            limit_memory="8Gi",
            limit_ephemeral_storage="1Gi",
        )

    @staticmethod
    def custom(
        request_cpu: str,
        request_memory: str,
        limit_memory: str,
        request_ephemeral_storage: Optional[str] = None,
        limit_ephemeral_storage: Optional[str] = "1Gi",
    ):
        """
        Generate a PodResources object with values specified manually.

        Request values are used by the Kubernetes load balancer when selecting a node for
        the pod to be run on. Limit values represent a hard cap on resource use. If the pod
        exceeds this, it will be killed by the Kubernetes scheduler. The limit value must
        be greater than the corresponding request value.

        CPU is specified in CPU units, with 1 CPU unit equivalent to one core. It can also be specified in millicpu,
        where for example, "100m" is equivalent to 0.1 units.

        Memory is specified in bytes, and a suffix like Ti, Gi, Mi, Ki may be used.

        Ephemeral storage is local storage where application data is written to. It is
        specified in bytes like memory.

        See the `Kubernetes documentation on resources <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>`__
        for background and acceptable units.
        """
        return PodResources(
            request_memory=request_memory,
            request_cpu=request_cpu,
            limit_memory=limit_memory,
            limit_ephemeral_storage=limit_ephemeral_storage,
            request_ephemeral_storage=request_ephemeral_storage,
        )
