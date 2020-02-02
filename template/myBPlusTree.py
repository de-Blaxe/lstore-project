from .myBPlusNode import (
    Node, RootNode, InternalNode, LeafNode, SingleRootNode
)

class myBPlusTree:
    def __init__(self):
        self.root = RootNode()

    def get_node(self, key, cur_node):
        if isinstance(self.root, SingleRootNode):
            return self.root
        if len(self.root.data) == 0:
            # make new leaf node
            # attach leaf node to root
            leaf = LeafNode()
            parent = leaf.parent
            parent.data.append(leaf)
            parent.data.append(key)
            return leaf

        pass

    def insert(self, key, value):
        cur_node = self.get_node(key, self.root)
        # insert into cur_node
        if
        pass

