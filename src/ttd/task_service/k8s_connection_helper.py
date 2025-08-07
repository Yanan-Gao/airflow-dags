from abc import abstractmethod, ABC
from typing import Optional, Tuple
import os

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from kubernetes import config, client
from kubernetes.client import CoreV1Api

from ttd.cloud_provider import CloudProvider, CloudProviders


class K8sConnectionHelper(ABC):

    def __init__(self, connection_name: str):
        self.connection_name = connection_name

    @property
    def connection(self) -> Connection:
        return BaseHook.get_connection(self.connection_name)

    @abstractmethod
    def _get_host(self, **kwargs) -> str:
        """
        Host to connect to
        """

    @abstractmethod
    def _get_port(self, **kwargs) -> str:
        """
        Port to connect to
        """

    def _get_config_params(self) -> Tuple[str, str]:
        connection = self.connection
        host = "https://" + config.incluster_config._join_host_port(
            self._get_host(connection=connection), self._get_port(connection=connection)
        )
        token = connection.password
        return host, token

    def get_k8s_api_client(self) -> CoreV1Api:
        host, token = self._get_config_params()
        configuration = client.Configuration()
        configuration.host = host
        configuration.api_key = {"authorization": token}
        configuration.api_key_prefix = {"authorization": "Bearer"}
        configuration.verify_ssl = False
        return client.CoreV1Api(client.ApiClient(configuration))


class AwsK8sConnectionHelper(K8sConnectionHelper):

    def _get_host(self, **kwargs) -> str:
        return os.environ[config.incluster_config.SERVICE_HOST_ENV_NAME]

    def _get_port(self, **kwargs) -> str:
        return os.environ[config.incluster_config.SERVICE_PORT_ENV_NAME]


class AzureK8sConnectionHelper(K8sConnectionHelper):

    def _get_host(self, connection) -> str:
        return connection.host

    def _get_port(self, connection) -> str:
        return connection.port


class AliCloudK8sConnectionHelper(K8sConnectionHelper):

    def _get_host(self, connection) -> str:
        return connection.host

    def _get_port(self, connection) -> str:
        return connection.port


class K8sSovereignConnectionHelper:

    def __init__(
        self,
        cloud_provider: CloudProvider,
        copy_pre_init: bool,
        deployer: K8sConnectionHelper,
        cleaner: K8sConnectionHelper,
        executer: Optional[K8sConnectionHelper] = None,
    ):
        self.cloud_provider = cloud_provider
        self.copy_pre_init = copy_pre_init
        self.deployer = deployer
        self.executer = executer
        self.cleaner = cleaner


aws = K8sSovereignConnectionHelper(
    cloud_provider=CloudProviders.aws,
    copy_pre_init=False,
    deployer=AwsK8sConnectionHelper(connection_name="task_service_serviceaccount_deployer"),
    cleaner=AwsK8sConnectionHelper(connection_name="task_service_serviceaccount_cleaner"),
)

azure = K8sSovereignConnectionHelper(
    cloud_provider=CloudProviders.azure,
    copy_pre_init=True,
    deployer=AzureK8sConnectionHelper(connection_name="task_service_serviceaccount_deployer_azure"),
    executer=AzureK8sConnectionHelper(connection_name="task_service_serviceaccount_executer_azure"),
    cleaner=AzureK8sConnectionHelper(connection_name="task_service_serviceaccount_cleaner_azure"),
)

alicloud = K8sSovereignConnectionHelper(
    cloud_provider=CloudProviders.ali,
    copy_pre_init=False,
    deployer=AliCloudK8sConnectionHelper(connection_name="task_service_serviceaccount_deployer_alicloud"),
    executer=AliCloudK8sConnectionHelper(connection_name="task_service_serviceaccount_executer_alicloud"),
    cleaner=AliCloudK8sConnectionHelper(connection_name="task_service_serviceaccount_cleaner_alicloud"),
)
