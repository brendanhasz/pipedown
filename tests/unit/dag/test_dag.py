import os

import numpy as np
import pandas as pd

from pipedown.cross_validation.splitters import RandomSplitter
from pipedown.dag import DAG
from pipedown.nodes.base import Input, Model, Node, Primary
from pipedown.nodes.filters import Collate, ItemFilter


def test_dag_fit_run_and_fitrun():

    run_list = []
    fit_list = []

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

        def fit(self, X, y):
            fit_list.append(self._name)
            self.x_mean = X.mean()

        def run(self, X, y):
            run_list.append(self._name)
            return X + self.x_mean, y

    class MyDAG(DAG):
        def nodes(self):
            return {
                "input": Input(),
                "loader": MyLoader(),
                "primary": Primary(["a", "b"], "c"),
                "my_node1": MyNode("A"),
                "my_node2": MyNode("B"),
            }

        def edges(self):
            return {
                "primary": {"test": "input", "train": "loader"},
                "my_node1": "primary",
                "my_node2": "my_node1",
            }

    # Fit
    my_dag = MyDAG()
    dag_outputs = my_dag.fit(outputs="my_node2")
    assert dag_outputs is None
    assert isinstance(fit_list, list)
    assert isinstance(run_list, list)
    assert len(fit_list) == 2
    assert len(run_list) == 2
    assert "A" in fit_list
    assert "B" in fit_list
    assert "A" in run_list
    assert "B" in run_list

    # Run
    df = pd.DataFrame()
    df["a"] = np.random.randn(5)
    df["b"] = np.random.randn(5)
    xo, yo = my_dag.run(inputs={"input": df}, outputs="my_node2")
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 5
    assert yo is None
    assert isinstance(fit_list, list)
    assert isinstance(run_list, list)
    assert len(fit_list) == 2
    assert len(run_list) == 4
    assert "A" in fit_list
    assert "B" in fit_list
    assert "A" in run_list[2:]
    assert "B" in run_list[2:]

    # Fit run
    while len(fit_list) > 0:
        fit_list.pop()
    while len(run_list) > 0:
        run_list.pop()
    xo, yo = my_dag.fit_run(outputs="my_node2")
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 10
    assert xo.shape[1] == 2
    assert isinstance(yo, pd.Series)
    assert yo.shape[0] == 10
    assert isinstance(fit_list, list)
    assert isinstance(run_list, list)
    assert len(fit_list) == 2
    assert len(run_list) == 2
    assert "A" in fit_list
    assert "B" in fit_list
    assert "A" in run_list
    assert "B" in run_list


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


def test_dag_eval_order_with_empty():

    run_list = []

    class MyNode(Node):
        def __init__(self, name):
            self._name = name

        def run(self, X, y):
            run_list.append(self._name)
            return X + 1, y

    class MyDAG(DAG):
        def nodes(self):
            return {
                "input": Input(),
                "primary": Primary(["a", "b"], "c"),
                "item_filter_1": ItemFilter(lambda x: x["a"] < 3),
                "item_filter_2": ItemFilter(
                    lambda x: (x["a"] >= 3) & (x["a"] < 10)
                ),
                "my_node1": MyNode("A"),
                "my_node2": MyNode("B"),
                "collate": Collate(),
            }

        def edges(self):
            return {
                "primary": {"test": "input", "train": "input"},
                "item_filter_1": "primary",
                "item_filter_2": "primary",
                "my_node1": "item_filter_1",
                "my_node2": "item_filter_2",
                "collate": ["my_node1", "my_node2"],
            }

    # Data split into two separate branches then recombined
    df = pd.DataFrame()
    df["a"] = [1, 2, 3, 4, 5, 6]
    df["b"] = [10, 20, 30, 40, 50, 60]
    df["c"] = [10, 20, 30, 40, 50, 60]
    my_dag = MyDAG()
    xo, yo = my_dag.run({"input": df})
    assert len(run_list) == 2
    assert "A" in run_list
    assert "B" in run_list
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 6
    assert xo.shape[1] == 2
    assert xo["a"].iloc[0] == 2
    assert xo["a"].iloc[1] == 3
    assert xo["a"].iloc[2] == 4
    assert xo["a"].iloc[3] == 5
    assert xo["a"].iloc[4] == 6
    assert xo["a"].iloc[5] == 7
    assert xo["b"].iloc[0] == 11
    assert xo["b"].iloc[1] == 21
    assert xo["b"].iloc[2] == 31
    assert xo["b"].iloc[3] == 41
    assert xo["b"].iloc[4] == 51
    assert xo["b"].iloc[5] == 61

    # Reset the run list
    while len(run_list) > 0:
        run_list.pop()

    # Data split into two separate branches but one is never executed
    df = pd.DataFrame()
    df["a"] = [1, 2, 1, 2, 1, 2]
    df["b"] = [10, 20, 30, 40, 50, 60]
    df["c"] = [10, 20, 30, 40, 50, 60]
    my_dag = MyDAG()
    xo, yo = my_dag.run({"input": df})
    assert len(run_list) == 1
    assert "A" in run_list
    assert "B" not in run_list
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 6
    assert xo.shape[1] == 2
    assert xo["a"].iloc[0] == 2
    assert xo["a"].iloc[1] == 3
    assert xo["a"].iloc[2] == 2
    assert xo["a"].iloc[3] == 3
    assert xo["a"].iloc[4] == 2
    assert xo["a"].iloc[5] == 3
    assert xo["b"].iloc[0] == 11
    assert xo["b"].iloc[1] == 21
    assert xo["b"].iloc[2] == 31
    assert xo["b"].iloc[3] == 41
    assert xo["b"].iloc[4] == 51
    assert xo["b"].iloc[5] == 61

    # Reset the run list
    while len(run_list) > 0:
        run_list.pop()

    # Same but now there's less data at the end
    df = pd.DataFrame()
    df["a"] = [1, 2, 1, 2, 10, 20]
    df["b"] = [10, 20, 30, 40, 50, 60]
    df["c"] = [10, 20, 30, 40, 50, 60]
    my_dag = MyDAG()
    xo, yo = my_dag.run({"input": df})
    assert len(run_list) == 1
    assert "A" in run_list
    assert "B" not in run_list
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 4
    assert xo.shape[1] == 2
    assert xo["a"].iloc[0] == 2
    assert xo["a"].iloc[1] == 3
    assert xo["a"].iloc[2] == 2
    assert xo["a"].iloc[3] == 3
    assert xo["b"].iloc[0] == 11
    assert xo["b"].iloc[1] == 21
    assert xo["b"].iloc[2] == 31
    assert xo["b"].iloc[3] == 41


def test_dag_get_and_save_html():
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
                "my_node1": MyNode("A"),
                "my_node2": MyNode("B"),
            }

        def edges(self):
            return {
                "primary": {"test": "input", "train": "loader"},
                "my_node1": "primary",
                "my_node2": "my_node1",
            }

    # Create the DAG
    my_dag = MyDAG()

    # Get the HTML
    html = my_dag.get_html()
    assert isinstance(html, str)

    # Save the html
    if os.path.exists("test_dag_viewer.html"):
        os.remove("test_dag_viewer.html")
    assert not os.path.exists("test_dag_viewer.html")
    my_dag.save_html("test_dag_viewer.html")
    assert os.path.exists("test_dag_viewer.html")


def test_cv_predict():
    class MyLoader(Node):
        def run(self, *args):
            df = pd.DataFrame()
            df["a"] = [1, 2, 3, 4, 5, 6]
            df["b"] = [7, 8, 9, 10, 11, 12]
            df["c"] = [13, 14, 15, 16, 17, 18]
            return df

    class MyModel(Model):
        def __init__(self, add):
            self._add = add

        def fit(self, X, y):
            pass

        def predict(self, X):
            return X["a"] + X["b"] + self._add

    class MyDAG(DAG):
        def nodes(self):
            return {
                "input": Input(),
                "loader": MyLoader(),
                "primary": Primary(["a", "b"], "c"),
                "my_model": MyModel(1),
            }

        def edges(self):
            return {
                "primary": {"test": "input", "train": "loader"},
                "my_model": "primary",
            }

    # Instantiate dag
    my_dag = MyDAG()

    # Call cv_predict with defaults
    cv_splitter = RandomSplitter(n_folds=2)
    predictions = my_dag.cv_predict(cv_splitter=cv_splitter)

    assert isinstance(predictions, pd.DataFrame)
    assert predictions.shape[0] == 6
    assert predictions.shape[1] == 2
    assert "y_true" in predictions
    assert "my_model" in predictions
    assert predictions["y_true"].iloc[0] == 13
    assert predictions["y_true"].iloc[1] == 14
    assert predictions["y_true"].iloc[2] == 15
    assert predictions["y_true"].iloc[3] == 16
    assert predictions["y_true"].iloc[4] == 17
    assert predictions["y_true"].iloc[5] == 18
    assert predictions["my_model"].iloc[0] == 9
    assert predictions["my_model"].iloc[1] == 11
    assert predictions["my_model"].iloc[2] == 13
    assert predictions["my_model"].iloc[3] == 15
    assert predictions["my_model"].iloc[4] == 17
    assert predictions["my_model"].iloc[5] == 19


def test_cv_predict_multiple_models():
    class MyLoader(Node):
        def run(self, *args):
            df = pd.DataFrame()
            df["a"] = [1, 2, 3, 4, 5, 6]
            df["b"] = [7, 8, 9, 10, 11, 12]
            df["c"] = [13, 14, 15, 16, 17, 18]
            return df

    class MyModel(Model):
        def __init__(self, add):
            self._add = add

        def fit(self, X, y):
            pass

        def predict(self, X):
            return X["a"] + X["b"] + self._add

    class MyDAG(DAG):
        def nodes(self):
            return {
                "input": Input(),
                "loader": MyLoader(),
                "primary": Primary(["a", "b"], "c"),
                "item_filter_1": ItemFilter(lambda x: x["a"] < 4),
                "item_filter_2": ItemFilter(lambda x: x["a"] >= 4),
                "my_model1": MyModel(1),
                "my_model2": MyModel(2),
                "my_model3": MyModel(3),
                "collate": Collate(),
            }

        def edges(self):
            return {
                "primary": {"test": "input", "train": "loader"},
                "item_filter_1": "primary",
                "item_filter_2": "primary",
                "my_model1": "item_filter_1",
                "my_model2": "item_filter_2",
                "collate": ["my_model1", "my_model2"],
                "my_model3": "primary",
            }

    # Instantiate dag
    my_dag = MyDAG()

    # Call cv_predict with defaults
    cv_splitter = RandomSplitter(n_folds=2)
    predictions = my_dag.cv_predict(
        outputs=["collate", "my_model3"], cv_splitter=cv_splitter
    )

    assert isinstance(predictions, pd.DataFrame)
    assert predictions.shape[0] == 6
    assert predictions.shape[1] == 3
    assert "y_true" in predictions
    assert "collate" in predictions
    assert "my_model3" in predictions
    assert predictions["y_true"].iloc[0] == 13
    assert predictions["y_true"].iloc[1] == 14
    assert predictions["y_true"].iloc[2] == 15
    assert predictions["y_true"].iloc[3] == 16
    assert predictions["y_true"].iloc[4] == 17
    assert predictions["y_true"].iloc[5] == 18
    assert predictions["collate"].iloc[0] == 9
    assert predictions["collate"].iloc[1] == 11
    assert predictions["collate"].iloc[2] == 13
    assert predictions["collate"].iloc[3] == 16
    assert predictions["collate"].iloc[4] == 18
    assert predictions["collate"].iloc[5] == 20
    assert predictions["my_model3"].iloc[0] == 11
    assert predictions["my_model3"].iloc[1] == 13
    assert predictions["my_model3"].iloc[2] == 15
    assert predictions["my_model3"].iloc[3] == 17
    assert predictions["my_model3"].iloc[4] == 19
    assert predictions["my_model3"].iloc[5] == 21
