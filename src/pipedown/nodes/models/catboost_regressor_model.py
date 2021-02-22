from typing import Optional, Tuple

import pandas as pd
from catboost import CatBoostRegressor

from pipedown.nodes.base import Model

class CatBoostRegressorModel(Model):
    """A CatBoost regression model


    Parameters
    ----------
    verbose : bool
        Whether to print information during training
    thread_count : int
        How many threads to use for fitting
    cat_features : List[str]
        List of feature names to treat as categorical
    loss_function : str
        What loss function to use.  Some examples include:

        * "RMSE"
        * "Quantile:alpha=0.5"
        * "Quantile:alpha=0.15"

    kwargs
        All keyword arguments (including the above) are passed to
        CatBoostRegressor.


    Examples
    --------

    ```python
    model = CatBoostRegressorModel(
        "model_name",
        verbose=False,
        thread_count=4,
        loss_function="Quantile:alpha=0.5",
    )
    ```
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.model = CatBoostRegressor(**kwargs)

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series]) -> None:
        self.model = self.model.fit(X, y)

    def run(
        self, X: pd.DataFrame, y: Optional[pd.Series]
    ) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
        return pd.Series(data=self.model.predict(X), index=X.index), y
