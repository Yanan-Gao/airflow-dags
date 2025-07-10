import re


class TrieNode:

    def __init__(self):
        self.children = {}
        self.feed_id = None


class FilePathTrie:

    def __init__(self):
        self.root = TrieNode()

    def insert(self, regex_path, feed_id):
        filepath = regex_path
        current_node = self.root
        components = filepath.split('/')
        for component in components:
            component = f"{component}$"
            if component not in current_node.children:
                current_node.children[component] = TrieNode()
            current_node = current_node.children[component]
        current_node.feed_id = feed_id

    def search(self, filepath):
        current_node = self.root
        components = filepath.split('/')
        for component in components:
            found_match = False
            for child_key, child_node in current_node.children.items():
                if re.match(child_key, component):
                    current_node = child_node
                    found_match = True
                    break
            if not found_match:
                return None
        return current_node.feed_id
