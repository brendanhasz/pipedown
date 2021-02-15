from typing import Optional, List

import pandas as pd

class FeatureFilter:

    def init(self, features: List[str]):
        self.features = features

    def run(self, X: pd.DataFrame, y: Optional[pd.Series]):
        return X[self.features], y
