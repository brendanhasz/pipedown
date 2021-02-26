import os
import uuid
from typing import Optional

import pandas as pd

from pipedown.nodes.base import Cache
from pipedown.utils.urls import get_node_url


class FeatherCache(Cache):
    """Cache data in a feather file.

    Note that this node requires the following packages:

    * [pyarrow](https://pypi.org/project/pyarrow/)
    """

    CODE_URL = get_node_url("caches/feather_cache.py")

    def __init__(self, filename: Optional[str] = None):
        if filename is not None:
            self.filename = filename
        else:
            self.filename = f"FeatherCache-{uuid.uuid4()}.feather"

    def fit(self, data: pd.DataFrame) -> None:
        if not self.is_cached():
            data.reset_index(drop=True).to_feather(self.filename)

    def run(self, data: Optional[pd.DataFrame]) -> pd.DataFrame:
        if self.is_cached():
            return pd.read_feather(self.filename)
        else:
            return data

    def is_cached(self) -> bool:
        return os.path.exists(self.filename) and os.path.isfile(self.filename)

    def clear_cache(self) -> None:
        os.remove(self.filename)
