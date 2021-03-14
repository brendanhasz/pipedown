from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd


class CrossValidationSplitter(ABC):
    """Abstract base class for a cross-validation splitter"""

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def setup(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        X : pd.DataFrame
            Features for the entire dataset
        y : pd.Series
            Target for the entire dataset
        """

    @abstractmethod
    def get_n_folds(self):
        """Get the number of folds"""

    @abstractmethod
    def get_fold(
        self, X: pd.DataFrame, y: pd.Series, i: int
    ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
        """Get one fold of data.

        Parameters
        ----------
        X : pd.DataFrame
            Features for the entire dataset
        y : pd.Series
            Target for the entire dataset
        i : int
            Index of the cross-validation fold to return.

        Returns
        -------
        x_train : pd.DataFrame
            Training features for fold i
        y_train : pd.DataFrame
            Training target for fold i
        x_val : pd.DataFrame
            Validation features for fold i
        y_val : pd.DataFrame
            Validation features for fold i
        """
