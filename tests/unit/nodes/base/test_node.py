import pandas as pd
import pytest

from pipedown.nodes.base.node import Node


def test_node():

    # It's an ABC, so this shouldn't work
    with pytest.raises(TypeError):
        node = Node()

    class MyBadNode(Node):
        def fit(self, X, y):
            pass

    # Didn't implement run(), so this shouldn't work
    with pytest.raises(TypeError):
        node = MyBadNode()

    class MyNode(Node):
        def fit(self, X, y):
            self.lala = "some_value"

        def run(self, X, y):
            X += 1
            y += 2
            return X, y

    # This *should* work
    node = MyNode()
    node.reset_connections()
    assert len(node._parents) == 0

    X = pd.DataFrame()
    X["a"] = [1, 2, 3]
    X["b"] = [4, 5, 6]
    y = pd.Series([7, 8, 9])

    # fit should store new attribs in the node
    assert not hasattr(node, "lala")
    node.fit(X, y)
    assert hasattr(node, "lala")
    assert node.lala == "some_value"

    # Run should return changed dfs
    xo, yo = node.run(X, y)
    assert isinstance(xo, pd.DataFrame)
    assert isinstance(yo, pd.Series)
    assert xo.shape[0] == 3
    assert xo.shape[1] == 2
    assert yo.shape[0] == 3
    assert xo.iloc[0, 0] == 2
    assert xo.iloc[1, 0] == 3
    assert xo.iloc[2, 0] == 4
    assert xo.iloc[0, 1] == 5
    assert xo.iloc[1, 1] == 6
    assert xo.iloc[2, 1] == 7
    assert yo.iloc[0] == 9
    assert yo.iloc[1] == 10
    assert yo.iloc[2] == 11

    # Setting parents
    node2 = MyNode()
    node3 = MyNode()
    node4 = MyNode()
    for n in [node2, node3, node4]:
        n.reset_connections()
    node.set_parents(node2)
    assert isinstance(node.get_parents(), list)
    assert node2 in node.get_parents()
    assert node.num_parents() == 1
    node.set_parents([node3, node4])
    assert isinstance(node.get_parents(), list)
    assert node2 not in node.get_parents()
    assert node3 in node.get_parents()
    assert node4 in node.get_parents()
    assert node.num_parents() == 2

    # Adding children
    node.add_children(node2)
    assert isinstance(node2.get_children(), set)
    assert node2 in node.get_children()
    assert node.num_children() == 1
