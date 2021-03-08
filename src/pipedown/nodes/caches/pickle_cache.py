import os
import pickle

from pipedown.nodes.base import Cache
from pipedown.utils.urls import get_node_url


class PickleCache(Cache):
    """Cache data in a pickle file"""

    CODE_URL = get_node_url("caches/feather_cache.py")

    def __init__(self, filename: str):
        self.filename = filename

    def fit(self, *args):
        with open(self.filename, "wb") as fid:
            pickle.dump(args, fid)

    def run(self, *args):
        if self.is_cached():
            with open(self.filename, "rb") as fid:
                return pickle.load(fid)
        else:
            return args

    def is_cached(self) -> bool:
        return os.path.exists(self.filename) and os.path.isfile(self.filename)

    def clear_cache(self) -> None:
        os.remove(self.filename)
