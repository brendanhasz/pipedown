import pandas as pd

from pipedown.dag import DAG, load_dag
from pipedown.nodes.base import Input, Node, Primary
from pipedown.nodes.filters import ItemFilter


def test_dag_save_and_load(is_close):
    class MyLoader(Node):
        def run(self, *args):
            df = pd.DataFrame()
            df["a"] = [1, 2, 3, 4, 5]
            df["b"] = [6, 7, 8, 9, 10]
            df["c"] = [11, 12, 13, 14, 15]
            return df

    class MyNode(Node):
        def __init__(self, name):
            self._name = name

        def fit(self, X, y):
            self.x_mean = X.mean()

        def run(self, X, y):
            return X + self.x_mean, y

    class MyDAG(DAG):
        def nodes(self):
            return {
                "input": Input(),
                "loader": MyLoader(),
                "primary": Primary(["a", "b"], "c"),
                "item_filter_1": ItemFilter(lambda x: x["a"] < 4),
                "item_filter_2": ItemFilter(lambda x: x["a"] > 3),
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

    # Instantiate the DAG
    my_dag = MyDAG()

    # Save it before fitting
    my_dag.save("test_dag_1.pkl")

    # Fit it
    my_dag.fit()
    assert is_close(my_dag.get_node("my_node1").x_mean["a"], 2)
    assert is_close(my_dag.get_node("my_node1").x_mean["b"], 7)
    assert is_close(my_dag.get_node("my_node2").x_mean["a"], 4.5)
    assert is_close(my_dag.get_node("my_node2").x_mean["b"], 9.5)

    # Save it after fitting
    my_dag.save("test_dag_2.pkl")

    # Load it back in
    loaded_dag = load_dag("test_dag_2.pkl")
    assert isinstance(loaded_dag, DAG)
    assert is_close(loaded_dag.get_node("my_node1").x_mean["a"], 2)
    assert is_close(loaded_dag.get_node("my_node1").x_mean["b"], 7)
    assert is_close(loaded_dag.get_node("my_node2").x_mean["a"], 4.5)
    assert is_close(loaded_dag.get_node("my_node2").x_mean["b"], 9.5)
