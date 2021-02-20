from copy import deepcopy
from typing import Any, List

import pandas as pd

from pipedown.cross_validation.splitters import CrossValidationSplitter

from .cross_validation_implementation import CrossValidationImplementation


class Sequential(CrossValidationImplementation):
    """Sequential, single-threaded, cross validation"""

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

        # Setup
        output_values = []
        splitter.setup(df)

        # Run each fold sequentially
        for i in range(splitter.get_n_folds()):

            # Get data for this fold
            tdf = deepcopy(splitter.get_fold(df, i))

            # Run the pipeline for this fold
            output_values.append(
                dag.run(inputs={dag.get_primary().name: tdf}, outputs=outputs)
            )

        return output_values
