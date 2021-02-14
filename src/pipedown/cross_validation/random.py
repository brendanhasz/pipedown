from typing import Tuple

import pandas as pd

from .cross_validator import CrossValidator


class RandomCrossValidator(CrossValidator):
    def __init__(self, n_splits=5, random_seed=12345):
        self.n_splits = n_splits
        self.random_seed = random_seed

    def setup(self, df: pd.DataFrame) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        df : pd.DataFrame
            The entire dataset
        """
        # TODO

    def get_n_folds(self):
        return self.n_splits

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
        # TODO
