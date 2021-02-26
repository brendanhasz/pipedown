from typing import Dict

import pandas as pd

from pipedown.nodes.base import Node
from pipedown.utils.urls import get_node_url


class RenameFields(Node):
    """Rename fields / columns

    Parameters
    ----------
    field_map : Dict[str, str]
        Dictionary with fields to rename and their new names.  Keys should be
        the old/existing field name, and the corresponding values should be the
        new names.
    """

    CODE_URL = get_node_url("transforms/rename_fields.py")

    def __init__(self, field_map: Dict[str, str]):
        self.field_map = field_map

    def run(self, X: pd.DataFrame):
        for old_name, new_name in self.field_map.items():
            X.rename(columns=self.field_map, inplace=True)
            return X
