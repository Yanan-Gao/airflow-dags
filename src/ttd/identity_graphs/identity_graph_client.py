from ttd.datasets.dataset import SUCCESS
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.identity_graphs.tree_node import TreeNode
from ttd.cloud_provider import CloudProvider


class IdentityGraphClient:
    """
    Client for a particular identity graph.

    WARNING: If you're an identity graph consumer, please do not create an instance of this class yourself, use
    the `ttd.identity_graphs.identity_graphs.IdentityGraphs` configuration class instead.
    """

    def __init__(self, node: TreeNode, dataset: DateGeneratedDataset):
        self._node = node
        node.data = self
        self._dataset = dataset

    def _get_spark_conf(self, cloud: CloudProvider):
        """
        The identity graph's variant configuration.
        """
        variant_path = self._node.get_path(separator=".")
        spark_conf = {}
        spark_conf[variant_path + ".location"] = self.dataset.with_cloud(cloud).get_dataset_path()
        return spark_conf

    @property
    def dataset(self) -> DateGeneratedDataset:
        """
        Returns TTD dataset that allows to perform operations such as wait and copy.
        """
        return self._dataset

    @staticmethod
    def _etled_identity_graph_client(node: TreeNode, name_path_segment: str, is_copied_to_azure: bool = False):
        """
        NOTE: "ETLed", "pre-ETL" and "post-ETL" is internal terminology: do not use it in consumer-facing API.
        """
        return IdentityGraphClient(
            node=node,
            dataset=DateGeneratedDataset(
                bucket="thetradedesk-useast-data-import",
                azure_bucket="ttd-identity@ttdexportdata" if is_copied_to_azure else None,
                path_prefix="sxd-etl/universal",
                data_name=name_path_segment,
                date_format="%Y-%m-%d/success",
                version=None,
                success_file=SUCCESS,
                env_aware=False
            )
        )
