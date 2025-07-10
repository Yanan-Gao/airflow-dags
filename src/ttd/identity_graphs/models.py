"""
This set of classes provides a way to configure a client for a particular identity graph.
None of the *Model and inherited classes have actionable API: they are only for getting
the needed instance of `ttd.identity_graphs.identity_graph.IdentityGraphClient`.
"""
from ttd.identity_graphs.identity_graph_client import IdentityGraphClient
from ttd.identity_graphs.tree_node import TreeNode
from abc import ABC, abstractmethod


class Model(ABC):

    def __init__(self, node: TreeNode):
        self._node = node

    @abstractmethod
    def default_client(self):
        ...


class InputModel(Model):

    def __init__(self, node: TreeNode):
        super().__init__(node)

    @abstractmethod
    def default_variant(self) -> IdentityGraphClient:
        """
        The variant is the leaf-level of the hieararchy, so the `default_client` and `default_variant` methods
        are synonyms.
        """


class VersionModel(Model):

    def __init__(self, node: TreeNode):
        super().__init__(node)

    @abstractmethod
    def default_input(self) -> InputModel:
        ...


class ProductModel(Model):

    def __init__(self, node: TreeNode):
        super().__init__(node)

    @abstractmethod
    def default_version(self) -> VersionModel:
        ...
