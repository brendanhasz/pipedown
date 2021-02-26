from typing import Callable, Optional

import pandas as pd

from pipedown.nodes.base import Node
from pipedown.utils.empty import EMPTY
from pipedown.utils.urls import get_node_url


class ItemFilter(Node):
    """Filter datapoints down to a subset matching some condition"""

    CODE_URL = get_node_url("filters/item_filter.py")

    def __init__(self, filter_function: Callable):
        self.filter_function = filter_function

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        ix = self.filter_function(X)
        if ix.sum() == 0:
            return EMPTY
        elif y is None:
            return X.loc[ix, :], None
        else:
            return X.loc[ix, :], y.loc[ix]
