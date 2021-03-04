import numpy as np
import pandas as pd

from pipedown.dag import DAG
from pipedown.nodes.base import Input, Node, Primary
from pipedown.nodes.filters import ItemFilter


def test_dag_default_outputs():
    class MyLoader(Node):
        def run(self, *args):
            df = pd.DataFrame()
            df["a"] = np.random.randn(10)
            df["b"] = np.random.randn(10)
            df["c"] = np.random.randn(10)
            return df

    class MyNode(Node):
        def __init__(self, name):
            self._name = name

        def run(self, X, y):
            return X + 1, y

    class MyDAG(DAG):
        def nodes(self):
            return {
                "input": Input(),
                "loader": MyLoader(),
                "primary": Primary(["a", "b"], "c"),
                "item_filter_1": ItemFilter(lambda x: x["a"] < 3),
                "item_filter_2": ItemFilter(
                    lambda x: (x["a"] >= 3) & (x["a"] < 10)
                ),
                "my_node1": MyNode("A"),
                "my_node2": MyNode("B"),
            }

        def edges(self):
            return {
                "primary": {"test": "input", "train": "loader"},
                "item_filter_1": "primary",
                "item_filter_2": "primary",
                "my_node1": "item_filter_1",
                "my_node2": "item_filter_2",
            }

    # During training, default outputs should be my_node1 and 2 (not Input)
    my_dag = MyDAG()
    my_dag.instantiate_dag("train")
    def_outputs = my_dag.get_default_outputs("train")
    assert isinstance(def_outputs, list)
    assert len(def_outputs) == 2
    assert "my_node1" in def_outputs
    assert "my_node2" in def_outputs

    # Same thing for test (output should not be loader)
    my_dag = MyDAG()
    my_dag.instantiate_dag("test")
    def_outputs = my_dag.get_default_outputs("test")
    assert isinstance(def_outputs, list)
    assert len(def_outputs) == 2
    assert "my_node1" in def_outputs
    assert "my_node2" in def_outputs
