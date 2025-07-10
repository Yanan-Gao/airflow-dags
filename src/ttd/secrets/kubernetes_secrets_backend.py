from base64 import b64decode
from functools import cached_property

from airflow.models import Connection
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret

from ttd.monads.maybe import Maybe, Just, Nothing

CONNECTION_SECRET_NAME = "airflow-connections"
VARIABLE_CONFIG_MAP_NAME = "airflow-variables"

ANSIBLE_MANAGED_SUFFIX = "ansible"
MANUAL_SUFFIX = "manual"


class K8sSecretsBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves connections and variables from the local K8s namespace
    """

    def __init__(self):
        super().__init__()

    @cached_property
    def namespace(self):
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
            return f.read()

    @cached_property
    def client(self):
        from kubernetes import config
        from kubernetes.client import CoreV1Api

        config.load_incluster_config()
        return CoreV1Api()

    def get_connection_from_secret(self, suffix: str, id: str) -> Maybe[Connection]:
        try:
            secret = self.client.read_namespaced_secret(name=f"{CONNECTION_SECRET_NAME}-{suffix}", namespace=self.namespace)
            uri = b64decode(secret.data[id]).decode()
            return Just(Connection(conn_id=id, uri=uri))
        except KeyError:
            return Nothing.unit()

    def get_variable_from_config_map(self, suffix: str, id: str) -> Maybe[str]:
        try:
            variables = self.client.read_namespaced_config_map(name=f"{VARIABLE_CONFIG_MAP_NAME}-{suffix}", namespace=self.namespace)
            return Just(variables.data[id])
        except KeyError:
            return Nothing.unit()

    def get_connection(self, conn_id: str) -> Connection | None:
        """
        Get connection from the CONNECTION_SECRET_NAME secrets in the current K8s namespace
        """
        result = (
            self.get_connection_from_secret(MANUAL_SUFFIX,
                                            conn_id).or_else(lambda: self.get_connection_from_secret(ANSIBLE_MANAGED_SUFFIX, conn_id))
        )
        if result.is_nothing():
            self.log.debug(f"Connection with id '{conn_id}' was not found in K8s backend")
        if result.is_just():
            mask_secret(result.get())
        return result.as_optional()

    def get_variable(self, key: str) -> str | None:
        """
        Get variable from the VARIABLE_CONFIG_MAP_NAME config map in the current K8s namespace
        """
        result = (
            self.get_variable_from_config_map(MANUAL_SUFFIX,
                                              key).or_else(lambda: self.get_variable_from_config_map(ANSIBLE_MANAGED_SUFFIX, key))
        )
        if result.is_nothing():
            self.log.debug(f"Variable with id '{key}' was not found in K8s backend")
        if result.is_just():
            mask_secret(result.get())
        return result.as_optional()
