from copy import deepcopy
from typing import Any, List, Union

import pandas as pd

from pipedown.cross_validation.splitters import CrossValidationSplitter

from .cross_validation_implementation import CrossValidationImplementation


class Sequential(CrossValidationImplementation):
    """Sequential, single-threaded, cross validation"""

    def run(
        self,
        dag,
        cv_on: str,
        X: pd.DataFrame,
        y: pd.Series,
        outputs: Union[str, List[str]],
        splitter: CrossValidationSplitter,
        verbose: bool = False,
    ) -> List[Any]:
        """Run the cross-validation

        Parameters
        ----------
        dag : pipedown.dag.DAG
            The DAG on which to perform cross validation
        cv_on : str
            Name of the node on whose outputs to cross-validate.
        X : pd.DataFrame
            Feature values output by cv_on node.
        y : pd.Series
            Target values output by cv_on node.
        outputs : Union[str, List[str]]
            Name(s) of the output node(s)
        splitter : CrossValidationSplitter object
            The splitter to use
        verbose : bool
            Whether to print info each fold
        """

        # Set up the splitter
        output_values = []
        splitter.setup(X, y)
        dag.instantiate_dag("train")
        cv_on_children = dag.get_node(cv_on).get_children()

        # Run each fold sequentially
        for i in range(splitter.get_n_folds()):

            # Get data for this fold
            x_train, y_train, x_val, y_val = deepcopy(
                splitter.get_fold(X, y, i)
            )

            # Fit the DAG on training data for this fold
            train_inputs = {c.name: (x_train, y_train) for c in cv_on_children}
            dag.fit(inputs=train_inputs, outputs=outputs)

            # Run the pipeline on validation data for this fold
            val_inputs = {c.name: (x_val, y_val) for c in cv_on_children}
            output_values.append(dag.run(inputs=val_inputs, outputs=outputs))

        return output_values
