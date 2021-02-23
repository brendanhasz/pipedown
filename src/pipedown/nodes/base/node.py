from abc import ABC, abstractmethod
from typing import List, Set, Union

from pipedown.visualization.node_drawers import rounded_box_fn_icon


class BaseNode(ABC):
    pass


class Node(BaseNode):

    draw = rounded_box_fn_icon

    def fit(self, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def set_parents(self, parents: Union[List[BaseNode], BaseNode]):
        if isinstance(parents, BaseNode):
            self._parents = [parents]
        else:
            self._parents = parents

    def get_parents(self) -> List[BaseNode]:
        return self._parents

    def num_parents(self) -> int:
        return len(self._parents)

    def reset_parents(self) -> None:
        self._parents = []

    def add_children(
        self, children: Union[List[BaseNode], Set[BaseNode], BaseNode]
    ):
        if isinstance(children, BaseNode):
            children = [children]
        for child in children:
            self._children.add(child)

    def get_children(self) -> Set[BaseNode]:
        return self._children

    def num_children(self) -> int:
        return len(self._children)

    def reset_children(self) -> None:
        self._children = set()

    def reset_connections(self) -> None:
        self.reset_parents()
        self.reset_children()
