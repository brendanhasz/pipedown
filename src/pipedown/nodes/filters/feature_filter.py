from typing import List, Optional

import pandas as pd

from pipedown.nodes.base.node import Node


class FeatureFilter(Node):
    def __init__(self, features: List[str]):
        self.features = features

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        return X[self.features], y
