from airflow.hooks.base import BaseHook
from opsapi.api.deployment_api import DeploymentApi
from opsapi.api.node_api import NodeApi
from opsapi.oauth_client_factory import OAuthAPIClientFactory
from typing import Type


class OpsApiHook(BaseHook):
    """
    Custom hook for opsplatform API using the service account for Airflow authentication.
    Optionally accepts an API client type (e.g., NodeApi, DeploymentApi).
    If not specified, defaults to DeploymentApi.
    """

    def __init__(self, conn_id: str, api_client_type: Type[DeploymentApi | NodeApi] = DeploymentApi):
        super().__init__()
        self.conn_id = conn_id
        self.client_id = "ops-platform-dev"
        self.client_secret = ""
        self.api_client_type = api_client_type
        self.client = self.get_client()

    def get_client(self):
        connection = self.get_connection(self.conn_id)

        return OAuthAPIClientFactory.get_oauth_client(
            self.api_client_type,
            connection.login,
            connection.password,
            self.client_id,
            self.client_secret,
            "https://ops-sso.adsrvr.org/auth/realms/master/protocol/openid-connect/token",
            "https://" + connection.host,
            verify_ssl=True
        )

    def get_active_releases(self):
        if isinstance(self.client, DeploymentApi):
            return self.client.deployment_active_releases_get()
        else:
            raise NotImplementedError("get_active_releases is only supported for DeploymentApi.")

    def get_active_deployments(self):
        if isinstance(self.client, DeploymentApi):
            return self.client.deployment_get_active_deployments()
        else:
            raise NotImplementedError("get_active_releases is only supported for DeploymentApi.")
