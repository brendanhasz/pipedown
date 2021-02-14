from abc import abstractmethod

from pipedown.visualization.node_drawers import rounded_box_metric_icon

from .node import Node


class Metric(Node):

    draw = rounded_box_metric_icon

    @abstractmethod
    def run(self, y_pred, y_true):
        pass

    @abstractmethod
    def get_metric_name(self):
        pass
