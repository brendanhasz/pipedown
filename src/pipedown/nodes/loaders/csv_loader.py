from typing import List, Optional

import pandas as pd

from pipedown.nodes.base.loader import Loader
from pipedown.utils.urls import get_node_url


class CsvLoader(Loader):
    """Loads data from a csv file

    Parameters
    ----------
    filename : str
        Filename of the csv file to load
    columns : List[str]
        Columns to load
    """

    CODE_URL = get_node_url("loaders/csv_loader.py")

    def __init__(self, filename: str, columns: Optional[List[str]], **kwargs):
        self.filename = filename
        self.columns = columns
        self.kwargs = kwargs

    def run(self):
        return pd.read_csv(self.filename, usecols=self.columns, **self.kwargs)
