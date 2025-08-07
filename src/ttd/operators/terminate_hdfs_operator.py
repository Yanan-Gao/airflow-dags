from functools import cached_property
from typing import Any

from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.context import Context
from kubernetes.client import AppsV1Api

from ttd.mixins.namespaced_mixin import NamespacedMixin


class TerminateHDFSOperator(NamespacedMixin, BaseOperator):

    def __init__(self, job_name: str, conn_id: str = "kubernetes_default", **kwargs):
        super().__init__(job_name=job_name, task_id=f"{job_name}-teardown-hdfs-cluster", **kwargs)
        self.conn_id = conn_id

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(conn_id=self.conn_id)

    @cached_property
    def app(self):
        return AppsV1Api(self.hook.api_client)

    def execute(self, context: Context) -> Any:

        namespace = self.get_namespace(context)
        namenode = None

        # Scale down both of the sets first, so that the associated PVCs are deleted. If they are outright wiped, then
        # K8s does not guarantee that the volumes are also deleted. Scaling them down guarantees this.
        label_str = ",".join([f"{key}={value}" for key, value in self.get_labels(context).items()])
        hdfs_sets = self.app.list_namespaced_stateful_set(namespace, label_selector=label_str)
        for stateful_set in hdfs_sets.items:
            stateful_set.spec.replicas = 0
            self.app.patch_namespaced_stateful_set_scale(stateful_set.metadata.name, namespace, stateful_set)

            # If we find the namenode, note it so that we can delete it and GC everything else after scaling down.
            try:
                if stateful_set.metadata.labels["hdfs/role"] == "namenode":
                    namenode = stateful_set
            except KeyError:
                pass

        if namenode is not None:
            self.app.delete_namespaced_stateful_set(namenode.metadata.name, namespace)
        else:
            self.log.warning("Couldn't find the namenode to tear down - everything else will be torn down regardless.")
            self._teardown_cluster(namespace, label_str)

        return None

    def _teardown_cluster(self, namespace: str, label_str: str):

        for list_func, delete_func in [(self.hook.core_v1_client.list_namespaced_service,
                                        self.hook.core_v1_client.delete_namespaced_service),
                                       (self.hook.core_v1_client.list_namespaced_config_map,
                                        self.hook.core_v1_client.delete_namespaced_config_map), (self.app.list_namespaced_stateful_set,
                                                                                                 self.app.delete_namespaced_stateful_set)]:
            objects = list_func(namespace, label_selector=label_str)
            for obj in objects:
                delete_func(obj.metadata.name, namespace)
