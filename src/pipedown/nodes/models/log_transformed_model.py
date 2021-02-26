import numpy as np
import pandas as pd

from pipedown.nodes.base import Model
from pipedown.utils.urls import get_node_url


class LogTransformedModel(Model):
    """Log transform and normalize the target variable before fitting


    Parameters
    ----------
    base_model
        Model to fit on the log-transformed target data


    Examples
    --------

    ```python
    model = LogTransformedModel(
        CatBoostRegressorModel(
            verbose=False,
            thread_count=4,
            loss_function="Quantile:alpha=0.5",
        )
    )
    ```
    """

    CODE_URL = get_node_url("models/log_transformed_model.py")

    def __init__(self, base_model):
        self.base_model = base_model
        self._mean = None
        self._std = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        log_y = np.log(y)
        self._mean = np.nanmean(log_y)
        self._std = np.nanstd(log_y)
        self.base_model.fit(X, (log_y - self._mean) / self._std)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        return np.exp(self.base_model.predict(X) * self._std + self._mean)
