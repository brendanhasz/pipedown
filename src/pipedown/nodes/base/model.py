from abc import abstractmethod

from pipedown.visualization.node_drawers import square_box_model_icon

from .node import Node


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
