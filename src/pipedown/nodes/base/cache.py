from abc import abstractmethod
from typing import Any

from .node import Node


class Cache(Node):
    @abstractmethod
    def fit(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def is_cached(self) -> bool:
        pass

    @abstractmethod
    def clear_cache(self) -> None:
        pass
