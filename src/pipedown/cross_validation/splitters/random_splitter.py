from typing import Tuple

import numpy as np
import pandas as pd

from .cross_validation_splitter import CrossValidationSplitter


class RandomSplitter(CrossValidationSplitter):
    """Perform random split cross validation

    Parameters
    ----------
    n_folds : int
        Total number of folds for cross-validation.  Default = 5
    random_seed : int
        Random seed to use for the random split.  Default = 12345
    """

    def __init__(self, n_folds: int = 5, random_seed: int = 12345):
        self.n_folds = n_folds
        self.random_seed = random_seed
        self.ix = None

    def setup(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Set up the cross-validation

        Parameters
        ----------
        X : pd.DataFrame
            Features for the entire dataset
        y : pd.Series
            Target for the entire dataset
        """
        rng = np.random.default_rng(self.random_seed)
        self.ix = rng.permutation(X.shape[0])
        self.n = X.shape[0]
        self.n_per_fold = np.floor(X.shape[0] / self.get_n_folds())

    def get_n_folds(self):
        return self.n_folds

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
        ix_0 = int(i * self.n_per_fold)
        if i + 1 == self.get_n_folds():
            ix_1 = X.shape[0]
        else:
            ix_1 = int((i + 1) * self.n_per_fold)
        ix_val = self.ix[ix_0:ix_1]
        ix_train = ~ix_val
        return (
            X.iloc[ix_train, :],
            y.iloc[ix_train],
            X.iloc[ix_val, :],
            y.iloc[ix_val],
        )
