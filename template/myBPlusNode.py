
class Node():
    def __init__(self):
        self.data = []
        self.parent = None
        self.max_children = 512

class RootNode(Node):
    def __init__(self):
        self.min_children = 2
        super().__init__()

class InternalNode(Node):
    def __init__(self):
        super().__init__()

    def split(self):
        # split data in half and put upper half in new InternalNode
        # Update parent of new node
        pass

class LeafNode(Node):
    def __init__(self):
        super().__init__()
        self.next_leaf = None

class SingleRootNode(Node):
    def __init__(self):
        super().__init__()
        self.max_children = 0
