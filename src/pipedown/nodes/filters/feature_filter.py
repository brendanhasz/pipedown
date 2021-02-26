from typing import List, Optional

import pandas as pd

from pipedown.nodes.base import Node
from pipedown.utils.urls import get_node_url


class FeatureFilter(Node):
    """Filter features / fields down to a specific subset"""

    CODE_URL = get_node_url("filters/feature_filter.py")

    def __init__(self, features: List[str]):
        self.features = features

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        return X[self.features], y
