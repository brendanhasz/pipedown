from abc import ABC, abstractmethod
from typing import Tuple

from .node import Node
from pipedown.visualization.node_drawers import square_box_model_icon


class Model(Node):

    self.draw = square_box_model_icon

    @abstractmethod
    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        pass

    @abstractmethod
    def predict(self, X: pd.DataFrame, y: pd.Series) -> pd.Series:
        pass

    def run(self, X, y):
        y_pred = self.predict(X)
        return y_pred, y
