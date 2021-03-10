from typing import List, Tuple, Union

import pandas as pd

from pipedown.nodes.base.node import Node
from pipedown.utils.urls import get_node_url


class Imputer(Node):
    """Impute missing values

    Parameters
    ----------
    features : None or str or List[str]
        Features to impute.  If None (the default), will impute missing values
        for all features.
    method : str {'mean' or 'median'}
        Method to use for imputation.

        * mean: fill missing values with the mean value for that feature
        * median: fill missing values with the median value for that feature
    """

    CODE_URL = get_node_url("transforms/imputer.py")

    continuous_methods = {
        "mean": lambda c: c.mean(),
        "median": lambda c: c.median(),
    }

    categorical_methods = {
        "mode": lambda c: c.mode().iloc[0],
    }

    categorical_dtypes = ["object", "category"]

    def __init__(
        self,
        features: Union[str, List[str]] = [],
        continuous_method: str = "mean",
        categorical_method: str = "mode",
    ):

        super().__init__()

        # Check methods are valid
        if continuous_method not in self.continuous_methods:
            raise ValueError(
                f"Invalid continuous_method '{continuous_method}'.  "
                f"Valid values are: {list(self.continuous_methods)}"
            )
        if categorical_method not in self.categorical_methods:
            raise ValueError(
                f"Invalid categorical_method '{categorical_method}'.  "
                f"Valid values are: {list(self.categorical_methods)}"
            )

        # Store attributes
        self.features = features if isinstance(features, list) else [features]
        self.continuous_method = self.continuous_methods[continuous_method]
        self.categorical_method = self.categorical_methods[categorical_method]

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:

        # Impute for all features by default
        if len(self.features) == 0:
            self.features = X.columns.tolist()

        # Compute the value to use for imputation
        self.values = {}
        for feature in self.features:
            if X[feature].dtype.name in self.categorical_dtypes:
                self.values[feature] = self.categorical_method(X[feature])
            else:
                self.values[feature] = self.continuous_method(X[feature])

    def run(
        self, X: pd.DataFrame, y: pd.Series
    ) -> Tuple[pd.DataFrame, pd.Series]:
        for feature in self.features:
            X.loc[X[feature].isnull(), feature] = self.values[feature]
        return X, y
