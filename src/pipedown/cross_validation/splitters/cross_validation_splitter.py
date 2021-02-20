from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd


class CrossValidationSplitter(ABC):
    """Abstract base class for a cross-validation splitter"""

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def setup(self, df: pd.DataFrame) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        df : pd.DataFrame
            The entire dataset
        """

    @abstractmethod
    def get_n_folds(self):
        """Get the number of folds"""

    @abstractmethod
    def get_fold(
        self, df: pd.DataFrame, i: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get one fold of data.

        Parameters
        ----------
        df : pd.DataFrame
            The entire dataset
        i : int
            Index of the cross-validation fold to return.

        Returns
        -------
        df_train : pd.DataFrame
            Training dataset for fold i
        df_val : pd.DataFrame
            Validation dataset for fold i
        """
