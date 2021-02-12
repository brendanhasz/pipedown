from abc import ABC, abstractmethod
from typing import Tuple

from pipedown.visualization.node_drawers import rounded_box_fn_icon


class Node(ABC):

    self.draw = rounded_box_fn_icon

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self._parents = []
        self.init(*args, **kwargs)

    def init(self, *args, **kwargs):
        pass

    def fit(self, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def set_parent(self, parents):
        if isinstance(parents, list):
            self._parents += parents
        else:
            self._parents += [parents]
