import pandas as pd

from pipedown.nodes.base.metric import Metric
from pipedown.utils.urls import get_node_url


class MeanSquaredError(Metric):
    CODE_URL = get_node_url("metrics/mean_squared_error.py")

    def run(self, y_pred: pd.Series, y_true: pd.Series):
        return ((y_pred - y_true).pow(2)).mean()

    def get_metric_name(self):
        return "mean_squared_error"
