from abc import ABC, abstractmethod
from typing import Any, List

import pandas as pd

from pipedown.cross_validation.splitters import CrossValidationSplitter


class CrossValidationImplementation(ABC):
    """Abstract base class for an implementation of cross-validation"""

    @abstractmethod
    def run(
        self, dag, df: pd.DataFrame, outputs, splitter: CrossValidationSplitter
    ) -> List[Any]:
        """Run the cross-validation

        Parameters
        ----------
        dag : pipedown.dag.DAG
            The DAG on which to perform cross validation
        df : pd.DataFrame
            The entire dataset to use as the input to the primary data node
        output : Union[str, List[str]]
            Name(s) of the output node(s)
        splitter : CrossValidationSplitter object
            The splitter to use
        """
