from typing import Callable, Optional

import pandas as pd

from pipedown.nodes.base import Node


class ItemFilter(Node):
    def __init__(self, filter_function: Callable):
        super().__init__()
        self.filter_function = filter_function

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        ix = self.filter_function(X)
        return X.loc[ix, :], y.loc[ix]
