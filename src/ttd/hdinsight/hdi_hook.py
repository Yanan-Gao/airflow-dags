import json
from datetime import datetime, timedelta
from typing import Dict, Optional, Union, Any, Iterable
from typing import List

from airflow.hooks.base import BaseHook
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import LROPoller
from azure.identity import ClientSecretCredential
from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.mgmt.hdinsight.models import (
    ClusterCreateProperties,
    ClusterCreateParametersExtended,
    ClusterIdentity,
    UserAssignedIdentity,
    _models,
    Cluster,
    ClusterPatchParameters,
)
from dateutil import parser

AZ_VERBOSE_LOGS_VAR = "AZ_VERBOSE_LOGS"
WARNING_LEVEL = "WARNING"


class HDIHook(BaseHook):

    def __init__(self, region: str = "eastus", conn_id="azure_service_account"):
        self.conn_id = conn_id
        self.region = region
        self.client = self.get_conn()

    def get_conn(self) -> HDInsightManagementClient:
        connection = self.get_connection(self.conn_id)
        extra = json.loads(connection.get_extra())

        credentials = ClientSecretCredential(
            client_id=extra["CLIENT_ID"],
            client_secret=connection.get_password(),  # type: ignore
            tenant_id=extra["TENANT_ID"],
        )
        return HDInsightManagementClient(credential=credentials, subscription_id=extra["SUBSCRIPTION_ID"])

    def get_extended_params(
        self,
        cluster_params: ClusterCreateProperties,
        cluster_tags: Dict[str, str],
        msi_resource_id: str,
    ) -> ClusterCreateParametersExtended:
        cluster_identity = ClusterIdentity(
            type="UserAssigned",
            user_assigned_identities={msi_resource_id: UserAssignedIdentity()},
        )
        return ClusterCreateParametersExtended(
            location=self.region,
            tags=cluster_tags,
            properties=cluster_params,
            identity=cluster_identity,
        )

    def get_clusters(self) -> Iterable[Cluster]:
        clusters = self.client.clusters.list()
        return clusters

    def get_old_clusters(
        self,
        resource_group: str,
        lifetime_hours: int = 24,
        filter_tags: Optional[Dict[str, str]] = None,
    ) -> List[Cluster]:
        filter_tags = filter_tags or {}
        clusters = self.client.clusters.list_by_resource_group(resource_group_name=resource_group)
        default_time_bound = datetime.now() - timedelta(hours=lifetime_hours)

        old_clusters = []
        for cluster in clusters:
            current_time_bound = default_time_bound
            if cluster.tags.get("ClusterLifetimeHours") is not None:
                tag_lifetime = min(lifetime_hours, int(cluster.tags.get("ClusterLifetimeHours")))
                current_time_bound = datetime.now() - timedelta(hours=tag_lifetime)

            if (parser.parse(cluster.properties.created_date) < current_time_bound and filter_tags.items() <= cluster.tags.items()
                    and cluster.properties.cluster_state != "Deleting"):
                old_clusters.append(cluster)

        return old_clusters

    def create_cluster(
        self,
        resource_group: str,
        cluster_name: str,
        cluster_params: ClusterCreateProperties,
        cluster_tags: Dict[str, str],
        msi_resource_id: str,
        wait_for_creation: bool = False,
    ) -> _models.Cluster:
        lro = self.client.clusters.begin_create(
            resource_group_name=resource_group,
            cluster_name=cluster_name,
            polling=wait_for_creation,
            parameters=self.get_extended_params(
                cluster_params=cluster_params,
                cluster_tags=cluster_tags,
                msi_resource_id=msi_resource_id,
            ),
        )
        return lro.result()

    def delete_cluster(self, cluster_name: str, cluster_config: Dict[str, str]) -> LROPoller[None]:
        return self.client.clusters.begin_delete(
            resource_group_name=cluster_config["resource_group"],
            cluster_name=cluster_name,
        )

    def delete_cluster2(self, resource_group: str, cluster_name: str, wait: bool = True) -> List[Union[Any, LROPoller[Any]]]:
        """
        Uses Azure API to send cluster deletion command.
        :param resource_group:
        :param cluster_name:
        :param wait: If True, waits for command to complete locking current thread. Default is True.
        :return: result of operation if wait, otherwise LROPoller
        """
        cluster_state = self.get_cluster_state(cluster_name, resource_group)
        self.log.info(f"The current cluster state is: {cluster_state}")

        lro = self.client.clusters.begin_delete(resource_group_name=resource_group, cluster_name=cluster_name)
        if wait:
            return lro.result()
        else:
            return lro

    def get_cluster_state(self, cluster_name: str, resource_group: str) -> str:
        state = self.get_cluster(cluster_name, resource_group).properties
        return state.cluster_state

    def get_cluster(self, cluster_name: str, resource_group: str) -> _models.Cluster:
        return self.client.clusters.get(
            resource_group_name=resource_group,
            cluster_name=cluster_name,  # , logger=logging.Logger("HDIClient", logging.WARNING)
        )

    def check_cluster_exists(self, cluster_name: str, resource_group: str) -> bool:
        cluster_exists = False
        try:
            self.get_cluster_state(cluster_name, resource_group)
            cluster_exists = True
        except ResourceNotFoundError:
            self.log.warning(f"Cluster {cluster_name} doesn't exist in the {resource_group} resource group")

        return cluster_exists

    def update_cluster_tags(self, cluster_name: str, resource_group: str, tags: Dict[str, str]) -> Cluster:
        cluster = self.client.clusters.get(cluster_name=cluster_name, resource_group_name=resource_group)
        return self.client.clusters.update(
            cluster_name=cluster_name,
            resource_group_name=resource_group,
            parameters=ClusterPatchParameters(tags={
                **cluster.tags,
                **tags
            }),
        )
