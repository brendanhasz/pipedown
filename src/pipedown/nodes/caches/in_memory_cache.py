from typing import Optional

import pandas as pd

from pipedown.nodes.base import Cache
from pipedown.utils.urls import get_node_url


class InMemoryCache(Cache):
    """Cache data in memory"""

    CODE_URL = get_node_url("caches/in_memory_cache.py")

    def __init__(self):
        self._data = None

    def fit(self, data: Optional[pd.DataFrame] = None) -> None:
        if self._data is None:
            self._data = data

    def run(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if self.is_cached():
            return self._data
        else:
            return data

    def is_cached(self) -> bool:
        return self._data is not None

    def clear_cache(self) -> None:
        del self._data
        self._data = None
