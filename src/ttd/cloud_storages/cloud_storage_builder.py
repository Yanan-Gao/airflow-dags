from typing import Optional

from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.cloud_storages.ali_cloud_storage import AliCloudStorage
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_storages.azure_cloud_storage import AzureCloudStorage
from ttd.cloud_storages.cloud_storage import CloudStorage


class CloudStorageBuilder:

    def __init__(self, cloud_provider: CloudProvider):
        self._conn_id: Optional[str] = None
        self._endpoint: Optional[str] = None
        self._cloud_provider = cloud_provider

    def set_conn_id(self, conn_id: str) -> "CloudStorageBuilder":
        self._conn_id = conn_id
        return self

    def set_endpoint(self, endpoint: str) -> "CloudStorageBuilder":
        self._endpoint = endpoint
        return self

    def build(self) -> CloudStorage:
        if self._cloud_provider == CloudProviders.aws:
            return AwsCloudStorage(self._conn_id)
        elif self._cloud_provider == CloudProviders.azure:
            return AzureCloudStorage(self._conn_id)
        elif self._cloud_provider == CloudProviders.ali:
            return AliCloudStorage(self._conn_id, self._endpoint)
        else:
            raise CloudStorageBuilderException(f"Invalid cloud provider - {self._cloud_provider}")


class CloudStorageBuilderException(Exception):
    pass
