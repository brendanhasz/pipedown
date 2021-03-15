from datetime import datetime
from typing import List, Tuple

import pandas as pd

from .cross_validation_splitter import CrossValidationSplitter


class TimeBinSplitter(CrossValidationSplitter):
    """Perform cross validation split using specific time bins

    Parameters
    ----------
    time_col : str
        Column / feature containing the time of each datapoint to split by
    time_bins : List[Tuple[datetime, datetime, datetime, datetime]]
        The time bins.  A list of (train_start, train_end, val_start, val_end)
        tuples where each time is a datetime object.
    """

    def __init__(
        self,
        time_col: str,
        time_bins: List[Tuple[datetime, datetime, datetime, datetime]],
    ):
        self.time_col = time_col
        self.time_bins = time_bins

    def setup(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        X : pd.DataFrame
            Features for the entire dataset
        y : pd.Series
            Target for the entire dataset
        """

    def get_n_folds(self):
        """Get the number of folds

        Returns
        -------
        n_folds : int
            The number of folds
        """
        return len(self.time_bins)

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
        train_ix = (X[self.time_col] > self.time_bins[i][0]) & (
            X[self.time_col] < self.time_bins[i][1]
        )
        val_ix = (X[self.time_col] > self.time_bins[i][2]) & (
            X[self.time_col] < self.time_bins[i][3]
        )
        return X[train_ix], y[train_ix], X[val_ix], y[val_ix]
