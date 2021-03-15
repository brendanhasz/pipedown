import numpy as np
import pandas as pd

import pipedown
from pipedown.dag import DAG
from pipedown.nodes.base import Input, Primary
from pipedown.nodes.metrics import MeanSquaredError


def test_readme_dag():
    class MeanImputer(pipedown.nodes.base.Node):
        def fit(self, X: pd.DataFrame, y: pd.Series):
            self.means = X.mean()

        def run(self, X: pd.DataFrame, y: pd.Series):
            for c in X:
                X[X[c].isnull()] = self.means[c]
            return X, y

    class LoadFromCsv(pipedown.nodes.base.Node):
        def __init__(self, filename: str):
            self.filename = filename

        def run(self):
            df = pd.DataFrame()
            df["feature1"] = np.random.randn(100)
            df["feature2"] = np.random.randn(100)
            df["target"] = np.random.randn(100)
            return df

    class LinearRegression(pipedown.nodes.base.Model):
        def fit(self, X: pd.DataFrame, y: pd.Series):
            xx = X.values
            yy = y.values.reshape((-1, 1))
            self.weights = np.linalg.inv(xx.T @ xx) @ xx.T @ yy

        def predict(self, X: pd.DataFrame):
            y_pred = X.values @ self.weights
            return pd.Series(data=y_pred.ravel(), index=X.index)

    class MyModel(DAG):
        def nodes(self):
            return {
                "load_csv": LoadFromCsv("some_csv.csv"),
                "test_input": Input(),
                "primary": Primary(["feature1", "feature2"], "target"),
                "imputer": MeanImputer(),
                "lr": LinearRegression(),
                "mse": MeanSquaredError(),
            }

        def edges(self):
            return {
                "mse": "lr",
                "lr": "imputer",
                "imputer": "primary",
                "primary": {"test": "test_input", "train": "load_csv"},
            }

    model = MyModel()
    model.fit()

    test_input = {"feature1": 1.2, "feature2": 3.4}
    model.run(inputs={"test_input": test_input}, outputs="lr")

    _ = model.cv_predict()

    model.cv_metric()

    model.save("test_readme_model.pkl")
    _ = pipedown.dag.io.load_dag("test_readme_model.pkl")

    # Get the raw html
    _ = model.get_html()

    # Or, save to html file:
    model.save_html("test_readme_model.html")
