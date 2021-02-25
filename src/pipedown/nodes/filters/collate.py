import pandas as pd

from pipedown.nodes.base import Node
from pipedown.utils.empty import is_empty


class Collate(Node):
    """Collate multiple data streams into a single one"""

    def run(self, *args):

        # Split into X and y
        X = []
        y = []
        for arg in args:
            if not is_empty(arg):
                if arg[0] is not None:
                    X.append(arg[0])
                if arg[1] is not None:
                    y.append(arg[1])

        # Concatenate X (should never be None)
        X = pd.concat(X).sort_index()

        # Concatenate y
        if len(y) == 0:
            y = None
        else:
            y = pd.concat(y).sort_index()

        return X, y
