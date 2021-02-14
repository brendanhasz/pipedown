from typing import List

import pandas as pd

from .node import Node
from pipedown.visualization.node_drawers import square_box_highlight


class Primary(Node):
    """Node to represent the primary data, and split it into X and y

    This node splits the data into the independent variables (X) and the
    dependent variable (y). It is just a marker for the point at which the
    testing + training data streams converge, and a marker for the point in the
    pipeline at which to cross-validate.

    Unlike normal nodes, this node takes two separate parents: one to be used
    during fitting (`train_parent`, when :meth:`.fit` is called), and one to be
    used during running (`test_parent`, when :meth:`.run` is called).

    Parameters
    ----------
    name : str
        Name of this node
    x : List[str]
        Columns / feature names of the independent variables
    y : str
        Column / feature name of the dependent variable
    """

    draw = square_box_highlight

    def init(self, x: List[str], y: str):
        self.x = x
        self.y = y

    def run(self, df: pd.DataFrame, mode: str):
        if mode == "test":
            return df[self.x], None
        else:
            return df[self.x], df[self.y]

    def set_parents(self, train_parent, test_parent):
        self._train_parent = train_parent
        self._test_parent = test_parent

    def get_train_parent(self):
        return self._train_parent

    def get_test_parent(self):
        return self._test_parent
