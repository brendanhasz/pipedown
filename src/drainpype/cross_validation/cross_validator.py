from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd


class CrossValidator(ABC):
    """Abstract base class for a cross-validation scheme"""

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def setup(self, X: pd.DataFrame, y) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        X : pd.DataFrame
            Entire dataset of independent variables
        y : pd.Series
            Entire dataset of dependent variables
        """
        pass

    @abstractmethod
    def get_fold(self, X: pd.DataFrame, y: pd.Series, i: int) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
        """Get one fold of data.

        Parameters
        ----------
        X : pd.DataFrame
            Entire dataset of independent variables
        y : pd.Series
            Entire dataset of dependent variables
        i : int
            Index of the cross-validation fold to return.

        Returns
        -------
        X_train : pd.DataFrame
            Training dataset of independent variables for this fold
        y_train : pd.Series
            Training dataset of dependent variables for this fold
        X_val : pd.DataFrame
            Validation dataset of independent variables for this fold
        y_val : pd.Series
            Validation dataset of dependent variables for this fold
        """
        pass
