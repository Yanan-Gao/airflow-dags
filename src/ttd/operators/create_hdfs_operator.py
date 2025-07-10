from xml.etree import ElementTree as ET
from functools import cached_property
from typing import Any, Optional

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_unique_suffix, POD_NAME_MAX_LENGTH
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context
from kubernetes.client import (
    V1StatefulSet, AppsV1Api, V1PodTemplateSpec, V1VolumeMount, V1Volume, V1ConfigMapVolumeSource, V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec, V1ResourceRequirements, V1Container, V1ContainerPort, V1ObjectMeta, V1StatefulSetSpec, V1LabelSelector,
    V1StatefulSetPersistentVolumeClaimRetentionPolicy, V1PodSpec, V1PodSecurityContext, V1OwnerReference, V1ConfigMap, V1Service,
    V1ServiceSpec, V1ServicePort
)

from ttd.mixins.namespaced_mixin import NamespacedMixin


class CreateHDFSOperator(NamespacedMixin, BaseSensorOperator):

    def __init__(
        self,
        job_name: str,
        num_datanodes: int,
        size_datanodes: int,
        conn_id: str = "kubernetes_default",
        hadoop_version: str = "3.3.2",
        **kwargs
    ):
        """
        Creates a HDFS cluster for writing data into. It generates a random suffix for the cluster, and passes the
        changed name through XCom. From there, the following objects are made:
        - {task_id}-{suffix}-nn - namenode stateful set
        - {task_id}-{suffix}-dn - datanode stateful set
        - {task_id}-{suffix}-nn-svc - namenode service
        - {task_id}-{suffix}-dn-svc - datanode service
        - {task_id}-{suffix}-conf-map - HDFS configmap

        :param job_name: The full name of the job to run.
        :param task_id:
        :param num_datanodes:
        :param size_datanodes:
        :param conn_id:
        :param kwargs:
        """
        super().__init__(job_name=job_name, task_id=f"{job_name}-create-hdfs-cluster", poke_interval=30, **kwargs)
        self.conn_id = conn_id
        self.hadoop_version = hadoop_version

        self.uid_name = add_unique_suffix(name=self.job_name, max_len=POD_NAME_MAX_LENGTH - 20)
        self.namenode_name = f"{self.uid_name}-nn"
        self.datanode_name = f"{self.uid_name}-dn"
        self.configmap_name = f"{self.uid_name}-conf-map"
        self.namenode_service = f"{self.namenode_name}-svc"
        self.datanode_name = f"{self.datanode_name}-svc"

        self.num_datanodes = num_datanodes
        self.size_datanodes = size_datanodes

    def execute(self, context: Context) -> Any:

        # Determine the namespace from the context first, so we can use it to create the K8s objects.
        namespace = self.get_namespace(context)

        # Create the StatefulSet for the namenode. This will claim ownership over the configmap and service.
        namenode_labels = self.get_labels(context)
        namenode_labels["hdfs/role"] = "namenode"
        namenode_ports = [("namenode-http", 9870), ("namenode-http-2", 9868), ("namenode-rpc", 8020)]
        namenode_set = self._fetch_or_create(
            self.app.list_namespaced_stateful_set, self.app.create_namespaced_stateful_set, self.app.read_namespaced_stateful_set,
            namespace, self._create_namenode(namenode_ports, context), namenode_labels
        )

        # Re-assign names in case we find a new cluster.
        self.namenode_name = namenode_set.metadata.name
        self.configmap_name = self._find_existing_config_map(namenode_set)

        owner_reference = self._build_owner_reference(namenode_set)

        core = self.hook.core_v1_client

        self._fetch_or_create(
            core.list_namespaced_config_map, core.create_namespaced_config_map, core.read_namespaced_config_map, namespace,
            self._create_config_map(namespace, owner_reference, context), self.get_labels(context)
        )
        self._fetch_or_create(
            core.list_namespaced_service, core.create_namespaced_service, core.read_namespaced_service, namespace,
            self._create_service("namenode", namenode_ports, owner_reference, context), namenode_labels
        )

        # Now that the namenode is fully set up, let's create the datanodes as well. The namenode will also have
        # ownership over it, so that they can be GC'd when the namenode is taken down too, reducing API calls to make.
        datanode_ports = [("datanode-http", 9864), ("datanode-data", 9866), ("datanode-ipc", 9867)]
        datanode_set = self._fetch_or_create(
            self.app.list_namespaced_stateful_set, self.app.create_namespaced_stateful_set, self.app.read_namespaced_stateful_set,
            namespace, self._create_datanodes(datanode_ports, owner_reference, context), self._get_labels("datanode", context)
        )
        self.datanode_name = datanode_set.metadata.name
        datanode_owner_reference = self._build_owner_reference(datanode_set)

        self._fetch_or_create(
            core.list_namespaced_service, core.create_namespaced_service, core.read_namespaced_service, namespace,
            self._create_service("datanode", datanode_ports, datanode_owner_reference, context), self._get_labels("datanode", context)
        )
        return super().execute(context)

    def poke(self, context: Context):

        namespace = self.get_namespace(context)

        # Before the operator finishes, let's wait until everything is up and running, so that when other tasks are
        # executed, they do not try to communicate with a pending HDFS setup.
        for hdfs_set in [self.namenode_name, self.datanode_name]:
            try:
                response = self.app.read_namespaced_stateful_set_status(hdfs_set, namespace)
                replica_count = response.spec.replicas
                ready_replicas = response.status.ready_replicas or 0

                self.log.info(f"{hdfs_set} has {ready_replicas} out of {replica_count} pods ready.")

                if ready_replicas < replica_count:
                    return False

            except KeyError:
                pass  # Presumably the status isn't ready for reading yet.

        self.log.info("Namenode and datanodes are ready.")
        return PokeReturnValue(is_done=True, xcom_value=self.configmap_name)

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(conn_id=self.conn_id)

    @cached_property
    def app(self):
        return AppsV1Api(self.hook.api_client)

    def get_hadoop_version(self):
        return f"proxy.docker.adsrvr.org/apache/hadoop:{self.hadoop_version}"

    def _create_config_map(self, namespace: str, owner_reference: V1OwnerReference, context: Context):

        return V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=V1ObjectMeta(name=f"{self.uid_name}-conf-map", owner_references=[owner_reference], labels=self.get_labels(context)),
            immutable=True,
            data={
                "hdfs-site.xml": self._build_hdfs_site(namespace),
                "core-site.xml": self._build_core_site(namespace)
            }
        )

    def _create_service(self, role: str, ports: list[tuple[str, int]], owner_reference: V1OwnerReference, context: Context):

        return V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(
                name=f"{self.uid_name}-{role[0]}{role[4]}-svc", owner_references=[owner_reference], labels=self._get_labels(role, context)
            ),
            spec=V1ServiceSpec(
                selector=self._get_labels(role, context),
                ports=[V1ServicePort(name=name, port=port, target_port=port) for (name, port) in ports]
            )
        )

    def _create_namenode(self, ports: list[tuple[str, int]], context: Context):
        labels = self._get_labels("namenode", context)

        return V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=V1ObjectMeta(name=self.namenode_name, labels=labels),
            spec=V1StatefulSetSpec(
                replicas=1,
                service_name="",
                selector=V1LabelSelector(match_labels=labels),
                persistent_volume_claim_retention_policy=
                V1StatefulSetPersistentVolumeClaimRetentionPolicy(when_deleted="Retain", when_scaled="Delete"),
                volume_claim_templates=[
                    V1PersistentVolumeClaim(
                        metadata=V1ObjectMeta(name="storage"),
                        spec=V1PersistentVolumeClaimSpec(
                            access_modes=["ReadWriteOnce"], resources=V1ResourceRequirements(requests={"storage": "5Gi"})
                        )
                    )
                ],
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels=labels),
                    spec=V1PodSpec(
                        security_context=V1PodSecurityContext(fs_group=100),
                        volumes=[V1Volume(name="hdfs-config", config_map=V1ConfigMapVolumeSource(name=self.configmap_name))],
                        init_containers=
                        [self._build_hdfs_container("hdfs-namenode-init", ["hdfs"], ["namenode", "-format", "-nonInteractive", "-force"])],
                        containers=[self._build_hdfs_container("hdfs-namenode", ["hdfs", "namenode"], ports=ports)]
                    )
                )
            )
        )

    def _create_datanodes(self, ports: list[tuple[str, int]], owner_reference: V1OwnerReference, context: Context):
        labels = self._get_labels("datanode", context)
        return V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=V1ObjectMeta(name=self.datanode_name, labels=labels, owner_references=[owner_reference]),
            spec=V1StatefulSetSpec(
                replicas=self.num_datanodes,
                service_name="",
                selector=V1LabelSelector(match_labels=labels),
                volume_claim_templates=[
                    V1PersistentVolumeClaim(
                        metadata=V1ObjectMeta(name="storage"),
                        spec=V1PersistentVolumeClaimSpec(
                            access_modes=["ReadWriteOnce"],
                            resources=V1ResourceRequirements(requests={"storage": f"{self.size_datanodes}Gi"})
                        )
                    )
                ],
                persistent_volume_claim_retention_policy=self._build_volume_retention_policy(),
                pod_management_policy="Parallel",
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels=labels),
                    spec=V1PodSpec(
                        security_context=V1PodSecurityContext(fs_group=100),
                        volumes=[V1Volume(name="hdfs-config", config_map=V1ConfigMapVolumeSource(name=f"{self.uid_name}-conf-map"))],
                        containers=[self._build_hdfs_container("hdfs-datanode", ["hdfs", "datanode"], ports=ports)]
                    )
                )
            )
        )

    def _build_hdfs_site(self, namespace: str) -> str:

        return self._build_xml_configuration([("dfs.namenode.name.dir", "/opt/hadoop/dfs/name"),
                                              ("dfs.namenode.rpc-address", f"{self.namenode_name}-svc.{namespace}.svc:8020"),
                                              ("dfs.namenode.rpc-bind-host", "0.0.0.0"),
                                              ("dfs.namenode.servicerpc-bind-host", "0.0.0.0:9000"),
                                              ("dfs.namenode.datanode.registration.ip-hostname-check", "false")])

    def _build_core_site(self, namespace: str):
        return self._build_xml_configuration([("fs.defaultFS", f"hdfs://{self.namenode_name}-svc.{namespace}.svc:8020"),
                                              ("hadoop.tmp.dir", "/opt/hadoop/tmp")])

    @staticmethod
    def _build_xml_configuration(properties: list[tuple[str, str]]) -> str:

        configuration = ET.Element("configuration")

        for (key, value) in properties:
            property_element = ET.SubElement(configuration, "property")
            name_element = ET.SubElement(property_element, "name")
            value_element = ET.SubElement(property_element, "value")

            name_element.text = key
            value_element.text = value

        return ET.tostring(configuration, encoding="unicode")

    def _build_hdfs_container(
        self, name: str, command: list[str], args: Optional[list[str]] = None, ports: Optional[list[tuple[str, int]]] = None
    ):
        return V1Container(
            image=self.get_hadoop_version(),
            name=name,
            command=command,
            args=args,
            volume_mounts=self._build_namenode_volume_mounts(),
            ports=[V1ContainerPort(name=name, container_port=port) for (name, port) in ports or []]
        )

    @staticmethod
    def _build_volume_retention_policy():
        return V1StatefulSetPersistentVolumeClaimRetentionPolicy(when_deleted="Retain", when_scaled="Delete")

    @staticmethod
    def _build_namenode_volume_mounts():
        return [
            V1VolumeMount(name="storage", mount_path="/opt/hadoop/dfs/name"),
            V1VolumeMount(name="hdfs-config", mount_path="/opt/hadoop/etc/hadoop")
        ]

    @staticmethod
    def _build_owner_reference(creation_response) -> V1OwnerReference:
        api_version = creation_response.api_version
        kind = creation_response.kind
        name = creation_response.metadata.name
        uid = creation_response.metadata.uid

        return V1OwnerReference(api_version=api_version, kind=kind, name=name, uid=uid, controller=True)

    def _get_labels(self, role: str, context: Context):
        labels = super().get_labels(context)
        labels["hdfs/role"] = role
        return labels

    def _find_existing_config_map(self, stateful_set: V1StatefulSet):
        volumes = stateful_set.spec.template.spec.volumes
        for volume in volumes:
            if volume.name != "hdfs-config":
                continue
            return volume.config_map.name
        return self.configmap_name
