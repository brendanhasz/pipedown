from abc import ABC, abstractmethod
from typing import Tuple

from .node import Node
from pipedown.visualization.node_drawers import square_box_json_icon


class Input(Node):

    self.draw = square_box_json_icon

    def run(self, data):
        # TODO: convert to dataframe from whatever is input
        # input can be dict, series, dataframe, list of dicts, or dict of lists
