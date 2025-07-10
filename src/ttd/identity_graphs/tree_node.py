from typing import Optional


class TreeNode:
    """
    Double-linked tree node.
    """

    def __init__(self, name: str, parent: Optional["TreeNode"] = None):
        self.name = name
        self.children: list[TreeNode] = []
        self.data = None
        if parent:
            parent.children.append(self)
        self.parent = parent

    def get_path(self, separator: str) -> str:
        """
        Path from the root of the tree to the current node.
        """
        node = self
        path = self.name
        while node.parent:
            node = node.parent
            path = node.name + separator + path
        return path
