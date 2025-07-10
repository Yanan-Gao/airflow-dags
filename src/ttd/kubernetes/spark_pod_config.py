from dataclasses import dataclass
from typing import Any, Optional

from kubernetes.client import V1Volume, V1VolumeMount

from ttd.kubernetes.k8s_instance_types import K8sInstanceType
from ttd.kubernetes.pod_resources import PodResources

NODE_LABEL = "node.kubernetes.io/instance-type"


def convert_python_types(value) -> str:
    match value:
        case True:
            return "true"
        case False:
            return "false"
        case None:
            return "null"
    return str(value)


@dataclass
class SparkGPUSpec:

    name: str
    quantity: int


@dataclass
class SparkPodResources(PodResources):
    """
    Resources to allocate to Spark job pods. Please note that when setting memory request and overhead, they are not
    formatted with the Kubernetes quantity format, but rather Spark's format (e.g. 1Gi becomes 1g).

    Args:
        limit_cpu: sets a hard limit on the number of cores to use. Can be formatted with milli-units (e.g. 900m or 0.9).
        memory_overhead: the amount of memory used as overhead.
        gpu_spec:
    """

    cores: int = 1
    limit_cpu: str | None = None
    memory_overhead: str = "1g"
    gpu_spec: Optional[SparkGPUSpec] = None


@dataclass
class SparkPodConfig:
    """
    The configuration for the Spark driver/executor pod. Each option provided is an option that both pods have in
    common.

    https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md#sparkoperator.k8s.io/v1beta2.SparkPodSpec

    Args:
        spark_pod_resources: Contains the spec configuration for the respective pod type.
        required_nodes: A list of instance types where the pods should be scheduled on at least one of.
        preferred_nodes: A list of instance types paired with weights, where the respective pod type would prefer to be
        scheduled on, not is not made a guarantee. The paired number represents weight, which is a number between 0 and
        100 (0 being no-op).
    """

    spark_pod_resources: SparkPodResources
    required_nodes: Optional[list[K8sInstanceType]] = None
    preferred_nodes: Optional[list[tuple[list[K8sInstanceType], int]]] = None

    def with_preferred_nodes(self, instances: list[K8sInstanceType], weight: int) -> "SparkPodConfig":

        if self.preferred_nodes is None:
            self.preferred_nodes = []
        self.preferred_nodes.append((instances, weight))
        return self

    def to_dict(self, opposite_role: str, extra_java_options: list[tuple[str, Any]], hdfs_id: Optional[str]) -> dict:
        final_dict = {}

        options = [
            ("cores", self.spark_pod_resources.cores), ("coreRequest", self.spark_pod_resources.request_cpu),
            ("coreLimit", self.spark_pod_resources.limit_cpu or self.spark_pod_resources.request_cpu),
            ("memory", self.spark_pod_resources.request_memory), ("memoryOverhead", self.spark_pod_resources.memory_overhead),
            (
                "gpu", {
                    "name": self.spark_pod_resources.gpu_spec.name,
                    "quantity": self.spark_pod_resources.gpu_spec.quantity
                } if self.spark_pod_resources.gpu_spec is not None else None
            ),
            (
                "javaOptions",
                self.build_java_args([(key, val) for (key, val) in extra_java_options if not key.startswith(f"spark.{opposite_role}")])
            ), ("affinity", self.build_node_affinity()),
            ("volumeMounts", [mount for mount in [self.create_volume_mount(), self.create_hdfs_mount(hdfs_id)] if mount is not None]),
            ("env", [env for env in self.create_hdfs_env()] if hdfs_id else None)
        ]

        for key, value in options:
            if value is not None:
                final_dict[key] = value

        return final_dict

    def build_node_affinity(self) -> dict | None:
        if self.preferred_nodes is None and self.required_nodes is None:
            return None

        affinity: dict[str, dict] = {"nodeAffinity": {}}
        if self.preferred_nodes is not None:
            affinity["nodeAffinity"]["preferredDuringSchedulingIgnoredDuringExecution"] = self.preferred_nodes_to_affinity()

        if self.required_nodes is not None:
            affinity["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"] = self.required_nodes_to_affinity()

        return affinity

    def required_nodes_to_affinity(self) -> dict:
        return {
            "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": NODE_LABEL,
                    "operator": "In",
                    "values": [node.instance_name for node in self.required_nodes or []]
                }]
            }]
        }

    def preferred_nodes_to_affinity(self) -> list:
        return [{
            "weight": preference[1],
            "preference": {
                "matchExpressions": [{
                    "key": NODE_LABEL,
                    "operator": "In",
                    "values": [node.instance_name for node in preference[0]]
                }]
            }
        } for preference in self.preferred_nodes or []]

    def build_java_args(self, java_args: list[tuple[str, Any]]) -> str:
        return " ".join([f"-D{key}={convert_python_types(value)}" for key, value in java_args])

    def create_volume(self) -> Optional[V1Volume]:
        ephemeral_volume_config = self.spark_pod_resources.ephemeral_volume_config

        return ephemeral_volume_config.create_volume() if ephemeral_volume_config is not None else {
            "name": "spark-local-dir-1",
            "emptyDir": {
                "sizeLimit": self.spark_pod_resources.limit_ephemeral_storage
            }
        }

    def create_volume_mount(self) -> Optional[V1VolumeMount]:
        ephemeral_volume_config = self.spark_pod_resources.ephemeral_volume_config

        return ephemeral_volume_config.create_volume_mount() if ephemeral_volume_config is not None else {
            "name": "storage",
            "mountPath": "/var/data"
        }

    @staticmethod
    def create_hdfs_mount(hdfs_id: Optional[str]) -> Optional[dict]:
        return {"name": "hdfs-config", "mountPath": "/etc/hadoop/conf"} if hdfs_id else None

    @staticmethod
    def create_hdfs_env() -> list[dict[str, str]]:
        return [{"name": "HADOOP_CONF_DIR", "value": "/etc/hadoop/conf"}, {"name": "SPARK_USER", "value": "hadoop"}]


class SparkExecutorPodConfig:

    def __init__(self, pod_config: SparkPodConfig, instances: int):
        self.pod_config = pod_config
        self.instances = instances

    def to_dict(self, extra_java_options: list[tuple[str, Any]], hdfs_id: str) -> dict:
        current_dict = self.pod_config.to_dict("driver", extra_java_options, hdfs_id)
        current_dict["instances"] = self.instances
        return current_dict
