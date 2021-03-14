from copy import deepcopy
from typing import Any, Dict, List

import pandas as pd

from pipedown.cross_validation.splitters import CrossValidationSplitter

from .cross_validation_implementation import CrossValidationImplementation


class Sequential(CrossValidationImplementation):
    """Sequential, single-threaded, cross validation"""

    def run(
        self, dag, cv_data: Dict[str, pd.DataFrame], outputs, splitter: CrossValidationSplitter, verbose: bool = False,
    ) -> List[Any]:
        """Run the cross-validation

        Parameters
        ----------
        dag : pipedown.dag.DAG
            The DAG on which to perform cross validation
        cv_data : Dict[str, Tuple[pd.DataFrame, pd.Series]]
            Input data to the nodes on which to cross-validate.
        output : Union[str, List[str]]
            Name(s) of the output node(s)
        splitter : CrossValidationSplitter object
            The splitter to use
        """

        # Get inputs
        cv_on = next(iter(cv_data))
        X = cv_data[cv_on][0]
        y = cv_data[cv_on][1]
        inputs = {c.name: (X, y) for c in dag.get_node(cv_on).get_children()}

        # Set up the splitter
        output_values = []
        splitter.setup(X, y)

        # Run each fold sequentially
        for i in range(splitter.get_n_folds()):

            # Get data for this fold
            x_train, y_train, x_val, y_val = deepcopy(splitter.get_fold(X, y, i))

            # Run the pipeline for this fold
            output_values.append(
                dag.run(inputs=inputs, outputs=outputs)
            )

        return output_values
