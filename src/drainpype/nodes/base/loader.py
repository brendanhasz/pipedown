from abc import ABC, abstractmethod
from typing import Tuple

from .node import Node
from drainpype.visualization.node_drawers import square_box_database_icon


class Loader(Node):

    self.draw = square_box_database_icon
