from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd

from .cross_validator import CrossValidator


class RandomCrossValidator(CrossValidator):
    
    def __init__(self, n_splits=5, random_seed=12345):
        self.n_splits = n_splits
        self.random_seed = random_seed

    def setup(self, X: pd.DataFrame, y) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        X : pd.DataFrame
            Entire dataset of independent variables
        y : pd.Series
            Entire dataset of dependent variables
        """
        # TODO

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
        #TODO

