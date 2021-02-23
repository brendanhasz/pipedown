import pandas as pd

from pipedown.nodes.base import Node


class Collate(Node):
    """Collate multiple data streams into a single dataframe"""

    def run(self, *args):
        return (
            pd.concat([e[i] for e in args]).sort_index()
            for i in range(len(args[0]))
        )
