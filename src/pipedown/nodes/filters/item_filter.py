from typing import Optional, Callable

import pandas as pd

class ItemFilter:

    def init(self, filter_function: Callable):
        self.filter_function = filter_function

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        ix = filter_function(X)
        return X.loc[ix, :], y.loc[ix]
