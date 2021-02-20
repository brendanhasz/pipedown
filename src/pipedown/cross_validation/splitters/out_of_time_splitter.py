from typing import Tuple

import pandas as pd

from .cross_validation_splitter import CrossValidationSplitter


class OutOfTimeSplitter(CrossValidationSplitter):
    """Perform out-of-time cross validation

    TODO: explain what that means

    Parameters
    ----------
    time_col : str
        Column / feature containing the time of each datapoint to split by
    n_folds : int
        Total number of folds for cross-validation.  Default = 5
    start_fold : int
        Fold to start at.  E.g. if n_folds=3 and start_fold=1, will perform 2
        folds: the first will train on the first third of the data and test on
        the middle third, and the second will train on the first two thirds of
        the data and test on the last third.  Default = 1.
    """

    def __init__(self, time_col: str, n_folds: int = 5, start_fold: int = 1):
        self.time_col = time_col
        self.n_folds = n_folds
        self.start_fold = start_fold

    def setup(self, df: pd.DataFrame) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        df : pd.DataFrame
            The entire dataset
        """
        # TODO

    def get_n_folds(self):
        return self.n_folds

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
