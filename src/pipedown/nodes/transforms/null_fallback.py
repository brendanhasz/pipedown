from typing import List, Optional, Tuple

import pandas as pd


class NullFallback:
    """Replaces missing values with values from a different column

    Parameters
    ----------
    fallback_list : List[Tuple[str, str]]
        Fallbacks to use.  Should be a list of tuples, where the first element
        in the tuple is the feature name with potentially missing elements, and
        the second element in the tuple is the feature name to replace any
        missing values with.
    n : int
        Number of times to perform the replacements sequentially (doing
        repeated times will propagate the replacements)
    """

    def __init__(self, fallback_list: List[Tuple[str, str]], n: int = 1):
        self.fallback_list = fallback_list
        self.n = n

    def run(
        self, X: pd.DataFrame, y: Optional[pd.Series]
    ) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
        for _ in range(self.n):
            for missing, less_missing in self.fallback_list:
                ix = X[missing].isnull()
                X.loc[ix, missing] = X.loc[ix, less_missing]
        return X, y
