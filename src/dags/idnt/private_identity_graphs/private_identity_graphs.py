import logging
import inspect
from types import FrameType
from typing import cast
from ttd.identity_graphs.identity_graphs import IdentityGraphs
from ttd.identity_graphs.identity_graph_client import IdentityGraphClient
from ttd.identity_graphs.models import ProductModel, VersionModel, InputModel
from ttd.identity_graphs.tree_node import TreeNode
from ttd.datasets.dataset import SUCCESS
from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class PrivateIdentityGraphs(IdentityGraphs):
    """
    Configuration class that provides access to the identity graphs' clients for the IDNT team. Please see documentation
    for `ttd.identity_graphs.identity_graphs.IdentityGraphs` for more information.

    WARNING: For teams outside the IDNT team, the identity graphs defined in this client come with no warranty. Please
    use the graphs defined in `ttd.identity_graphs.identity_graphs.IdentityGraphs` for your workflows and contact the
    IDNT team for feature requests.
    """

    def __init__(self):
        if "dags/idnt/" not in inspect.getframeinfo(cast(FrameType, cast(FrameType, inspect.currentframe()).f_back)).filename:
            logging.warning(
                "PrivateIdentityGraphs is for use inside the IDNT team only. Use `ttd.identity_graphs.IdentityGraphs` instead and see the docstring for more information."
            )
        super().__init__()
        self._singleton_graph = self.SingletonGraph(TreeNode("singletonGraph", self._node))

    @property
    def singleton_graph(self):
        """
        Pending deprecation. Singleton graph is a part of deprecated TTD Graph V1 and its generation is going to be stopped in Q1 2025.
        Singletons are going to be added to TTD Graph V2.
        """
        logging.warning(
            "Singleton graph is a part of deprecated TTD Graph V1 and its generation is going to be stopped in Q1 2025. Singletons are going to be added to TTD Graph V2."
        )
        return self._singleton_graph

    class SingletonGraph(ProductModel):

        def __init__(self, node: TreeNode):
            super().__init__(node)
            self._v1 = self.V1(TreeNode("v1", node))

        @property
        def v1(self):
            return self._v1

        @property
        def default_version(self):
            return self.v1

        @property
        def default_client(self):
            return self.default_version.default_client

        class V1(VersionModel):

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
                    self._raw = IdentityGraphClient(
                        TreeNode("raw", self._node),
                        dataset=DateGeneratedDataset(
                            bucket="ttd-insights",
                            path_prefix="graph",
                            data_name="singletongraph",
                            date_format="%Y-%m-%d",
                            success_file=SUCCESS,
                            version=None,
                            env_aware=True
                        )
                    )

                @property
                def raw(self):
                    return self._raw

                @property
                def default_variant(self):
                    return self.raw

                @property
                def default_client(self):
                    return self.default_variant
