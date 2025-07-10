from enum import Enum

from typing import List

from ttd.task_service.k8s_connection_helper import (
    aws,
    azure,
    alicloud,
    K8sSovereignConnectionHelper,
)


class VerticaCluster(Enum):
    USEast01 = 5
    USWest01 = 9
    USEast02_UI = 15
    USWest02_UI = 16
    USEast03 = 24
    USWest03 = 25
    CNEast01 = 27
    CNWest01 = 28


class TaskVariantGroup(Enum):
    VerticaEtl = -8
    # VerticaLegacy = -9
    VerticaUI = -10
    VerticaAws = -11
    VerticaAzure = -12
    VerticaAliCloud = -13
    # Any = -1
    # Default = 0


vertica_cluster_variant_map = {
    TaskVariantGroup.VerticaAws: [VerticaCluster.USEast01, VerticaCluster.USWest01],
    TaskVariantGroup.VerticaAzure: [VerticaCluster.USEast03, VerticaCluster.USWest03],
    TaskVariantGroup.VerticaAliCloud: [VerticaCluster.CNEast01, VerticaCluster.CNWest01]
}

vertica_cluster_variant_map[TaskVariantGroup.VerticaEtl] = (
    vertica_cluster_variant_map[TaskVariantGroup.VerticaAws] + vertica_cluster_variant_map[TaskVariantGroup.VerticaAzure]
)

vertica_cluster_group_cloud_provider_map = {
    TaskVariantGroup.VerticaAws: aws,
    TaskVariantGroup.VerticaAzure: azure,
    TaskVariantGroup.VerticaAliCloud: alicloud
}

# maps defined vertica clusters to cloud providers, e.g. USEast01, USWest01 -> aws, USEast03, USWest03 -> azure
vertica_cluster_cloud_provider_map = {
    cluster: vertica_cluster_group_cloud_provider_map.get(cluster_group)
    for cluster_group, clusters in vertica_cluster_variant_map.items()
    for cluster in clusters if cluster_group in vertica_cluster_group_cloud_provider_map
}


def get_variants_for_group(group: TaskVariantGroup) -> List[VerticaCluster]:
    return vertica_cluster_variant_map.get(group)


def get_cloud_provider_for_cluster(cluster: VerticaCluster, ) -> K8sSovereignConnectionHelper:
    return vertica_cluster_cloud_provider_map.get(cluster)
