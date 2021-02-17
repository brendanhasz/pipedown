from typing import Optional, Callable

import pandas as pd

from pipedown.nodes.base.node import Node

class ItemFilter(Node):

    def init(self, filter_function: Callable):
        self.filter_function = filter_function

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        ix = self.filter_function(X)
        return X.loc[ix, :], y.loc[ix]
