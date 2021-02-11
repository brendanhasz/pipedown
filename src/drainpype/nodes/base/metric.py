from abc import ABC, abstractmethod
from typing import Tuple

from .node import Node
from drainpype.visualization.node_drawers import rounded_box_metric_icon


class Metric(Node):

    self.draw = rounded_box_metric_icon

    @abstractmethod
    def run(self, y_pred, y_true):
        pass

    @abstractmethod
    def get_metric_name(self):
        pass
