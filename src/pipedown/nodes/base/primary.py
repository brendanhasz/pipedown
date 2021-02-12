from abc import ABC, abstractmethod
from typing import Tuple

from .node import Node
from pipedown.visualization.node_drawers import square_box_highlight


class Primary(Node):
    """Node to represent the primary data 

    This node doesn't do anything, but is just a marker for the point at which
    the testing + training data streams converge, and as a marker for the point
    in the pipeline at which to cross-validate.
    """

    self.draw = square_box_highlight

    def run(self, *args):
        return *args  # does nothing, is just here for organization

    def set_parent(self, train_parent, test_parent):
        self._train_parent = train_parent
        self._test_parent = test_parent
