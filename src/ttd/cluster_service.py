from abc import ABC


class ClusterService(ABC):

    def __init__(self, cluster_service: str):
        self._cluster_service = cluster_service

    def __str__(self) -> str:
        return self._cluster_service

    def __hash__(self) -> int:
        return hash(self._cluster_service)

    def __eq__(self, other: object) -> bool:
        if other is None:
            return False
        if not isinstance(other, ClusterService):
            return NotImplemented
        return self._cluster_service == other._cluster_service

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)


class AwsEmr(ClusterService):

    def __init__(self):
        super(AwsEmr, self).__init__("AwsEMR")


class AzureHDInsight(ClusterService):

    def __init__(self):
        super(AzureHDInsight, self).__init__("HDInsight")


class AliCloudEMR(ClusterService):

    def __init__(self):
        super(AliCloudEMR, self).__init__("AliEMR")


class Databricks(ClusterService):

    def __init__(self):
        super(Databricks, self).__init__("Databricks")


class ClusterServices:
    AwsEmr: AwsEmr = AwsEmr()
    HDInsight: AzureHDInsight = AzureHDInsight()
    AliCloudEmr: AliCloudEMR = AliCloudEMR()
    Databricks: Databricks = Databricks()
