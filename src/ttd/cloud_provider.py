from abc import ABC

from ttd.cluster_service import ClusterService, ClusterServices


class CloudProvider(ABC):

    def __init__(self, provider: str):
        self._provider = provider

    def __str__(self) -> str:
        return self._provider

    def __hash__(self) -> int:
        return hash(self._provider)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CloudProvider):
            return NotImplemented
        return self._provider == other._provider

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)


class AwsCloudProvider(CloudProvider):

    def __init__(self):
        super(AwsCloudProvider, self).__init__(provider="aws")


class AzureCloudProvider(CloudProvider):

    def __init__(self):
        super(AzureCloudProvider, self).__init__(provider="azure")


class AliCloudProvider(CloudProvider):

    def __init__(self):
        super(AliCloudProvider, self).__init__(provider="alicloud")


class DatabricksCloudProvider(CloudProvider):

    def __init__(self):
        super(DatabricksCloudProvider, self).__init__(provider="databricks")


class CloudProviders:
    aws: AwsCloudProvider = AwsCloudProvider()
    azure: AzureCloudProvider = AzureCloudProvider()
    ali: AliCloudProvider = AliCloudProvider()
    databricks: DatabricksCloudProvider = DatabricksCloudProvider()


class CloudProviderMapper:
    _mapping = {
        CloudProviders.aws: ClusterServices.AwsEmr,
        CloudProviders.azure: ClusterServices.HDInsight,
        CloudProviders.ali: ClusterServices.AliCloudEmr,
        CloudProviders.databricks: ClusterServices.Databricks
    }

    @staticmethod
    def get_cluster_service(provider: CloudProvider) -> ClusterService | None:
        return CloudProviderMapper._mapping.get(provider)
