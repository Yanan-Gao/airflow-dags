import logging
from ttd.identity_graphs.identity_graph_client import IdentityGraphClient
from ttd.identity_graphs.models import ProductModel, VersionModel, InputModel
from ttd.identity_graphs.tree_node import TreeNode
from ttd.cloud_provider import CloudProvider, CloudProviders
from collections import deque


class IdentityGraphs:
    """
    Configuration class that provides access to identity graphs' clients.

    Identity Graphs FAQ page - https://thetradedesk.atlassian.net/l/cp/R1T9Fz2Q

    The purpose of this class is threefold:

    - To organise all the variety or identity graph into the easy to use hieararcical structure.
    - To provide identity graphs' migration and deprecation mechanism.
    - To explicitly list identity graphs meant for consumption outside of the IDNT team: the one that have comptibility guarantees and support.

    The hierarchical structure consists of the following entities - product (or graph name), algorithm (or version), input and variant (or transformations). See
    the textual representation of the hierarchy below. For convenience there is the `default_client` property on each level of the hierarchy that
    returns the identity graph.

    Example use:

        >>> from ttd.identity_graphs.identity_graphs import IdentityGraphs
        >>> from ttd.datasets.date_generated_dataset import DateGeneratedDataset
        >>>
        >>> identity_graphs = IdentityGraphs()
        >>> identity_alliance = identity_graphs.identity_alliance
        >>>
        >>> # Get the client object with the identity graph's TTD dataset object.
        >>> dataset: DateGeneratedDataset = identity_alliance.default_client.dataset
        >>>
        >>> # All the expressions below return the same client for the same identity graph.
        >>> identity_alliance.default_client
        >>> identity_alliance.v2.default_client
        >>> identity_alliance.v2.based_on_ttd_graph_v2.default_client
        >>> identity_alliance.v2.based_on_ttd_graph_v2.persons_capped_for_hot_cache_and_with_dats

    There might be multiple transformations applied to a single graph, the naming convention is
    to separate each transformation with `and` - `transformation1_and_transformation2_and_transformation3`.

    - Identity Alliance (product)
        - V2 (algorithm)
            - TTD Graph V2 and all standard partners' identity graphs (input)
                - Persons capped for HotCache, enriched with DATs, in Parquet format (variant)
                - Households capped for HotCache, enriched with DATs, in Parquet format (variant)
            - TTD Graph V1 and all standard partners' identity graphs (input). Deprecated.
                - Persons capped for HotCache, enriched with DATs, in Parquet format (variant)
                - Households capped for HotCache, enriched with DATs, in Parquet format (variant)
            - Cookieless TTD Graph V1 and all partners' identity graphs (input). Deprecated.
                - Persons capped for HotCache, enriched with DATs, in Parquet format (variant)
                - Households capped for HotCache, enriched with DATs, in Parquet format (variant)
    - TTD Graph (product)
        - V2, OpenGraph (algorithm)
            - Standard input
                - Persons capped for HotCache, in Parquet format
                - Households capped for HotCache, in Parquet format
        - V1, Adbrain (algorithm). Deprecated.
            - Standard input
                - Persons capped for HotCache, in Parquet format
                - Households capped for HotCache, in Parquet format
            - Cookieless input
                - Persons capped for HotCache, in Parquet format
                - Households capped for HotCache, in Parquet format
    """

    def __init__(self):
        # The root of the tree used for generating configuration for the Scala client library.
        self._node = TreeNode("spark.identityGraphs")

        self._identity_alliance = self.IdentityAllianceModel(TreeNode("identityAlliance", self._node))
        self._ttd_graph = self.TTDGraphModel(TreeNode("ttdGraph", self._node))
        self._live_ramp_graph = self.LiveRampGraphModel(TreeNode("liveRampGraph", self._node))

    @property
    def spark_defaults_classification(self):
        """
        The same as get_spark_conf but wrapped in AWS EMR spark-defaults classification.
        See https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html
        """
        return {"Classification": "spark-defaults", "Properties": self.get_spark_conf()}

    def get_spark_conf(self, cloud: CloudProvider = CloudProviders.aws) -> dict[str, str]:
        """
        The most up-to-date configuration for Identity Graphs Scala client library. Please *always*
        pass this to all your EMR clusters/Spark applications that need the identity graphs using
        one of the methods below. Using EMR cluster-wide configuration is preferable.

          * AWS EMR cluster-wide spark-defaults - EmrClusterTask.additional_application_configurations
          * Databricks cluster-wide defaults - DatabricksWorkflow.spark_configs
          * EMR step configuration - additional_args_option_pairs_list in EmrJobTask, HDIJobTask
            or AliCloudJobTask.

        :param cloud: Cloud to generate Spark configuration for. The default is AWS. For Databrics, use AWS.
        """
        spark_conf: dict[str, str] = {}
        q: deque[TreeNode] = deque()
        q.append(self._node)
        while q:
            node = q.popleft()
            if not node.children:
                spark_conf.update(node.data._get_spark_conf(cloud))
            else:
                q.extend(node.children)

        return spark_conf

    @property
    def identity_alliance(self):
        """
        Identity Alliance.

        The larger out of the two graphs. It's based on TTD Graph and multiple partners' identity graphs. Enriched with DATs (or MPS) for covering traffic with no user ID. This graph is probabilistic.
        """
        return self._identity_alliance

    @property
    def ttd_graph(self):
        """
        TTD Graph.

        The smaller out of the two graphs but more consistent in terms out of user ID to person or household assignment. This graph is probabilistic.
        """
        return self._ttd_graph

    @property
    def live_ramp_graph(self):
        """
        LiveRamp Graph. Also known as IdentityLink Graph.

        Please NOTE that this is a partner's graph, and its generation is not under TTD's control. Delays of its delivery, partial deliveries and
        sudded large changes in its content might be possible.
        """
        return self._live_ramp_graph

    @property
    def default_client(self):
        return self.identity_alliance.default_client

    class IdentityAllianceModel(ProductModel):

        def __init__(self, node: TreeNode):
            super().__init__(node)
            self._v2 = self.V2Model(TreeNode("v2", node))

        @property
        def v2(self):
            return self._v2

        @property
        def default_version(self):
            return self.v2

        @property
        def default_client(self):
            return self.default_version.default_client

        class V2Model(VersionModel):

            def __init__(self, node: TreeNode):
                super().__init__(node)
                self._based_on_ttd_graph_v2 = self.BasedOnTTDGraphV2Model(TreeNode("basedOnTtdGraphV2", node))
                self._based_on_ttd_graph_v1 = self.BasedOnTTDGraphV1Model(TreeNode("basedOnTtdGraphV1", node))

            @property
            def based_on_ttd_graph_v2(self):
                return self._based_on_ttd_graph_v2

            @property
            def based_on_ttd_graph_v1(self):
                """
                Deprecated. TTD Graph V1 is deprecated and its generation is going to be stopped in Q3 2025. Please use Identity Alliance based on TTD Graph V2 instead."
                """
                logging.warning(
                    "TTD Graph V1 is deprecated and its generation is going to be stopped in Q3 2025. Please use Identity Alliance based on TTD Graph V2 instead."
                )
                return self._based_on_ttd_graph_v1

            @property
            def default_input(self):
                return self.based_on_ttd_graph_v2

            @property
            def default_client(self):
                return self.default_input.default_client

            class BasedOnTTDGraphV2Model(InputModel):

                def __init__(self, node: TreeNode):
                    super().__init__(node)
                    self._persons_capped_for_hot_cache_and_with_dats = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("personsCappedForHotCacheAndWithDats", node), "iav2graph", is_copied_to_azure=True
                    )
                    self._households_capped_for_hot_cache_and_with_dats = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("householdsCappedForHotCacheAndWithDats", node), "iav2graph_household", is_copied_to_azure=True
                    )

                @property
                def persons_capped_for_hot_cache_and_with_dats(self):
                    return self._persons_capped_for_hot_cache_and_with_dats

                @property
                def households_capped_for_hot_cache_and_with_dats(self):
                    return self._households_capped_for_hot_cache_and_with_dats

                @property
                def default_variant(self):
                    return self.persons_capped_for_hot_cache_and_with_dats

                @property
                def default_client(self):
                    return self.default_variant

            class BasedOnTTDGraphV1Model(InputModel):

                def __init__(self, node: TreeNode):
                    super().__init__(node)
                    self._persons_capped_for_hot_cache_and_with_dats = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("personsCappedForHotCacheAndWithDats", node), "iav2graph_legacy"
                    )
                    self._households_capped_for_hot_cache_and_with_dats = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("householdsCappedForHotCacheAndWithDats", node), "iav2graph_household_legacy"
                    )

                @property
                def persons_capped_for_hot_cache_and_with_dats(self):
                    return self._persons_capped_for_hot_cache_and_with_dats

                @property
                def households_capped_for_hot_cache_and_with_dats(self):
                    return self._households_capped_for_hot_cache_and_with_dats

                @property
                def default_variant(self):
                    return self.persons_capped_for_hot_cache_and_with_dats

                @property
                def default_client(self):
                    return self.default_variant

    class TTDGraphModel(ProductModel):

        def __init__(self, node: TreeNode):
            super().__init__(node)
            self._v2 = self.V2Model(TreeNode("v2", node))
            self._v1 = self.V1Model(TreeNode("v1", node))

        @property
        def v2(self):
            return self._v2

        @property
        def default_version(self):
            return self.v2

        @property
        def default_client(self):
            return self.default_version.default_client

        class V2Model(VersionModel):

            def __init__(self, node: TreeNode):
                super().__init__(node)
                self._standard_input = self.StandardInput(TreeNode("standardInput", node))

            @property
            def standard_input(self):
                return self._standard_input

            @property
            def default_input(self):
                return self.standard_input

            @property
            def default_client(self):
                return self.default_input.default_client

            class StandardInput(InputModel):

                def __init__(self, node: TreeNode):
                    super().__init__(node)
                    self._persons_capped_for_hot_cache = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("personsCappedForHotCache", node), "nextgen"
                    )
                    self._households_capped_for_hot_cache = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("householdsCappedForHotCache", node), "nextgen_household"
                    )
                    self._singleton_persons = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("singletonPersons", node), "nextgen_singletons"
                    )

                @property
                def persons_capped_for_hot_cache(self):
                    return self._persons_capped_for_hot_cache

                @property
                def households_capped_for_hot_cache(self):
                    return self._households_capped_for_hot_cache

                @property
                def singleton_persons(self):
                    """
                    Persons that have a single user ID. Right now these are from APAC region only.
                    """
                    return self._singleton_persons

                @property
                def default_variant(self):
                    return self.persons_capped_for_hot_cache

                @property
                def default_client(self):
                    return self.default_variant

        @property
        def v1(self):
            """
            Deprecated. TTD Graph V1 and its generation is going to be stopped in Q3 2025. Please use V2 instead.
            """
            logging.warning("TTD Graph V1 is deprecated and its generation is going to be stopped in Q3 2025. Please use V2 instead.")
            return self._v1

        class V1Model(VersionModel):

            def __init__(self, node: TreeNode):
                super().__init__(node)
                self._standard_input = self.StandardInput(TreeNode("standardInput", node))

            @property
            def standard_input(self):
                return self._standard_input

            @property
            def default_input(self):
                return self.standard_input

            @property
            def default_client(self):
                return self.default_input.default_client

            class StandardInput(InputModel):

                def __init__(self, node: TreeNode):
                    super().__init__(node)
                    self._persons_capped_for_hot_cache = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("personsCappedForHotCache", node), "adbrain_legacy"
                    )
                    self._households_capped_for_hot_cache = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("householdsCappedForHotCache", node), "adbrain_household_legacy"
                    )

                @property
                def persons_capped_for_hot_cache(self):
                    return self._persons_capped_for_hot_cache

                @property
                def households_capped_for_hot_cache(self):
                    return self._households_capped_for_hot_cache

                @property
                def default_variant(self):
                    return self.persons_capped_for_hot_cache

                @property
                def default_client(self):
                    return self.default_variant

    class LiveRampGraphModel(ProductModel):

        def __init__(self, node: TreeNode):
            super().__init__(node)
            self._v1 = self.V1Model(TreeNode("v1", node))

        @property
        def v1(self):
            return self._v1

        @property
        def default_version(self):
            return self.v1

        @property
        def default_client(self):
            return self.default_version.default_client

        class V1Model(VersionModel):

            def __init__(self, node: TreeNode):
                super().__init__(node)
                self._merged = self.MergedModel(TreeNode("merged", node))

            @property
            def merged(self):
                """
                All LiveRamp's datasets for different user ID types and geographical regions merged together.
                """
                return self._merged

            @property
            def default_input(self):
                return self.merged

            @property
            def default_client(self):
                return self.default_input.default_client

            class MergedModel(InputModel):

                def __init__(self, node: TreeNode):
                    super().__init__(node)
                    self._persons_capped_for_hot_cache = IdentityGraphClient._etled_identity_graph_client(
                        TreeNode("personsCappedForHotCache", node), "identitylink"
                    )

                @property
                def persons_capped_for_hot_cache(self):
                    return self._persons_capped_for_hot_cache

                @property
                def default_variant(self):
                    return self.persons_capped_for_hot_cache

                @property
                def default_client(self):
                    return self.default_variant
