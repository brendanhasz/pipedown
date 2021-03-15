import numpy as np
import pandas as pd

from pipedown.cross_validation.implementations import Sequential
from pipedown.cross_validation.splitters import RandomSplitter
from pipedown.dag import DAG
from pipedown.nodes.base import Node


def test_sequential():

    fit_list = []
    run_list = []

    class MyNode(Node):
        def __init__(self, name):
            self._name = name

        def fit(self, X, y):
            fit_list.append(self._name)
            self.x_mean = X.mean()

        def run(self, X, y):
            run_list.append(self._name)
            return X + self.x_mean, y

    class MyDAG(DAG):
        def nodes(self):
            return {
                "my_node1": MyNode("a"),
                "my_node2": MyNode("b"),
                "my_node3": MyNode("c"),
            }

        def edges(self):
            return {
                "my_node2": "my_node1",
                "my_node3": "my_node2",
            }

    # Instantiate the dag
    my_dag = MyDAG()

    # Cross-validate on outputs of my_node1
    cv_on = "my_node1"
    X = pd.DataFrame()
    X["a"] = np.random.randn(10)
    X["b"] = np.random.randn(10)
    X["c"] = np.random.randn(10)
    y = pd.Series(np.random.randn(10))

    # Create the CV implementation object and do the CV
    scv = Sequential()
    outputs = scv.run(
        my_dag, cv_on, X, y, ["my_node3"], RandomSplitter(n_folds=5)
    )
    assert isinstance(outputs, list)
    assert len(outputs) == 5
    for output in outputs:
        assert isinstance(output, tuple)
        assert len(output) == 2
        assert isinstance(output[0], pd.DataFrame)
        assert isinstance(output[1], pd.Series)
        assert output[0].shape[0] == 2
        assert output[0].shape[1] == 3
        assert output[1].shape[0] == 2

    # Check it ran the nodes in the correct order
    # (ran nodes twice, once during dag.fit on training data,
    # and again during fit.run on val data)
    assert len(fit_list) == 10
    assert len(run_list) == 20
    fit_order = ["b", "c"] * 5
    for i in range(len(fit_order)):
        assert fit_list[i] == fit_order[i]
    run_order = ["b", "c"] * 10
    for i in range(len(run_order)):
        assert run_list[i] == run_order[i]
