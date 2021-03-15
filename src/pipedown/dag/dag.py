from abc import abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from pipedown.cross_validation.implementations import (
    CrossValidationImplementation,
    Sequential,
)
from pipedown.cross_validation.splitters import (
    CrossValidationSplitter,
    RandomSplitter,
)
from pipedown.dag.dag_tools import run_dag
from pipedown.dag.io import save_dag
from pipedown.nodes.base import Cache, Metric, Model, Node, Primary
from pipedown.visualization.dag_viewer import get_dag_viewer_html


class DAG:
    """Abstract base class for a DAG"""

    @abstractmethod
    def nodes(self) -> Dict[str, Node]:
        """Get a dict of node names and objects used in the DAG"""

    @abstractmethod
    def edges(self) -> Dict[str, Union[str, List[str]]]:
        """Get the edges between nodes in the DAG"""

    def fit(
        self, inputs: Dict[str, Any] = {}, outputs: Union[str, List[str]] = []
    ) -> None:
        """Fit part of or the whole pipeline

        Parameters
        ----------
        inputs : Dict[str, Any]
            Dict of the input data.  Keys should be node names, and values
            should be the data to use as inputs to those nodes.
        outputs : Union[str, List[str]]
            List of output nodes.

        Returns
        -------
        None
        """
        self.instantiate_dag("train")
        if len(outputs) == 0:  # default outputs are nodes w/o children
            outputs = self.get_default_outputs("train")
        run_dag(inputs, outputs, "train", self.get_nodes())

    def run(
        self, inputs: Dict[str, Any] = {}, outputs: Union[str, List[str]] = []
    ):
        """Run part of or the whole pipeline

        Parameters
        ----------
        inputs : Dict[str, Any]
            Dict of the input data.  Keys should be node names, and values
            should be the data to use as inputs to those nodes.
        outputs : Union[str, List[str]]
            List of output nodes.

        Returns
        -------
        output_data : Union[Any, Dict[str, Any]]
            If `outputs` was a str (a single output node), simply returns the
            output data from that node.  If `outputs` was a list of str,
            returns a Dict whose keys are output node names, and values are the
            output data for that node.
        """
        self.instantiate_dag("test")
        if len(outputs) == 0:  # default outputs are nodes w/o children
            outputs = self.get_default_outputs("train")
        return run_dag(inputs, outputs, "test", self.get_nodes())

    def fit_run(
        self, inputs: Dict[str, Any] = {}, outputs: Union[str, List[str]] = []
    ) -> Union[Any, Dict[str, Any]]:
        """Fit and run part of or the whole pipeline

        Parameters
        ----------
        inputs : Dict[str, Any]
            Dict of the input data.  Keys should be node names, and values
            should be the data to use as inputs to those nodes.
        outputs : Union[str, List[str]]
            List of output nodes.

        Returns
        -------
        output_data : Union[Any, Dict[str, Any]]
            If `outputs` was a str (a single output node), simply returns the
            output data from that node.  If `outputs` was a list of str,
            returns a Dict whose keys are output node names, and values are the
            output data for that node.
        """
        self.instantiate_dag("train")
        if len(outputs) == 0:  # default outputs are nodes w/o children
            outputs = self.get_default_outputs("train")
        return run_dag(inputs, outputs, "train", self.get_nodes())

    def instantiate_dag(self, mode: str):
        """Create nodes and connections between them"""

        # Clear pre-existing connections between nodes
        for node in self.get_nodes():
            node.reset_children()

        # Create connections between nodes depending on mode
        for child_name, parent_names in self.edges().items():

            # Get actual child and parent(s) objects
            child = self.get_node(child_name)
            if (
                isinstance(parent_names, str)
                and parent_names in self.get_node_dict()
            ):
                parents = [self.get_node(parent_names)]
            elif isinstance(parent_names, list) and all(
                p in self.get_node_dict() for p in parent_names
            ):
                parents = [self.get_node(p) for p in parent_names]
            elif (
                isinstance(parent_names, dict)
                and "test" in parent_names
                and "train" in parent_names
                and isinstance(child, Primary)
            ):
                parents = [self.get_node(parent_names[mode])]
            else:
                raise RuntimeError(f"Invalid parent(s) {parent_names}")

            # Assign parents + children
            child.set_parents(parents)
            for parent in parents:
                parent.add_children(child)

    def initialize_nodes(self):
        """Initialize the node objects"""
        self._nodes = self.nodes()
        for name, node in self._nodes.items():
            node.name = name
            node.reset_connections()

    def get_default_outputs(self, mode):
        """Get the default outputs (nodes w/o children)"""
        outputs = []
        primary_parents = self.edges()[self.get_primary().name]
        for node in self.get_nodes():
            if (
                node.num_children() == 0
                and node.name != primary_parents["train"]
                and node.name != primary_parents["test"]
            ):
                outputs.append(node.name)
        return outputs

    def get_node_dict(self) -> Dict[str, Node]:
        """Get a dict mapping node names to node objects"""
        if not hasattr(self, "_nodes") or self._nodes is None:
            self.initialize_nodes()
        return self._nodes

    def get_nodes(self, node_type=Node) -> List[Node]:
        """Get a list of all nodes contained in this pipeline"""
        return [
            n
            for n in self.get_node_dict().values()
            if isinstance(n, node_type)
        ]

    def get_node(self, node_name: str):
        """Get a node in the pipeline by its name"""
        return self.get_node_dict()[node_name]

    def get_primary(self) -> Node:
        """Get the primary node if it exists in the DAG"""
        primaries = self.get_nodes(Primary)
        if len(primaries) < 1:
            return None
        elif len(primaries) == 1:
            return primaries[0]
        else:
            raise RuntimeError(
                "There are multiple Primary data sources in your DAG!"
            )

    def clear_caches(self):
        """Clear all cache nodes in the DAG"""
        for cache in self.get_nodes(Cache):
            cache.clear_cache()

    def cv_predict(
        self,
        inputs: Dict[str, Any] = {},
        outputs: Union[str, List[str]] = [],
        cv_on: Optional[Union[str, List[str]]] = None,
        cv_splitter: CrossValidationSplitter = RandomSplitter(),
        cv_implementation: CrossValidationImplementation = Sequential(),
        verbose=False,
    ):
        """Make cross-validated predictions using the pipeline

        Parameters
        ----------
        inputs : None or pd.DataFrame
            Input to use, if any.  By default will just run the whole pipeline
            up to the model node(s) specified in `outputs`.
        outputs : List[str]
            List of names of the model nodes from which to get predictions.
        cv_on : Optional[Union[str, List[str]]]
            Node(s) on which to cross-validate.  By default uses the Primary.
        cv_splitter : CrossValidationSplitter object
            Cross-validation scheme to use.
        cv_implementation : CrossValidationImplementation object
            Cross-validation implementation to use.
        verbose : bool
            Whether to show fold times

        Returns
        -------
        pd.DataFrame
            Cross-validated predictions.  Contains columns with the true target
            values (column `y_true`), and then one column for each of the
            models in `outputs` list, with that model's predictions.
        """

        # Default is to run all models in the pipeline
        if isinstance(outputs, str):
            outputs = [outputs]
        if len(outputs) == 0:
            outputs = [n.name for n in self.get_nodes(Model)]

        # Run the pipeline up to the node to cross validate on
        X, y, original_index = self.run_to_cv_node(inputs, cv_on)

        # Run the cross-validation
        predictions = cv_implementation.run(
            self, cv_on, X, y, outputs, cv_splitter, verbose=verbose
        )

        # Return the collated predictions
        if isinstance(outputs, (list, set)) and len(outputs) > 1:
            output_predictions = []
            for i, output in enumerate(outputs):
                output_predictions += [pd.concat([p[i] for p in predictions])]
                output_predictions[i].sort_index(inplace=True)
                output_predictions[i].set_index(original_index)
        else:
            output_predictions = pd.concat(predictions)
            output_predictions.sort_index(inplace=True)
            output_predictions.set_index(original_index)
        return output_predictions

    def cv_metric(
        self,
        inputs: Dict[str, Any] = {},
        outputs: Union[str, List[str]] = [],
        cv_on: Optional[Union[str, List[str]]] = None,
        cv_splitter: CrossValidationSplitter = RandomSplitter(),
        cv_implementation: CrossValidationImplementation = Sequential(),
        verbose=False,
    ):
        """Compute metric(s) from cross-validated predictions

        Parameters
        ----------
        inputs : None or pd.DataFrame
            Input to use as the primary data.  By default will just run the
            whole pipeline up to the metric node(s) specified in `outputs`.
        outputs : List[str]
            List of names of the metric nodes to evaluate.
        cv_on : Optional[Union[str, List[str]]]
            Node(s) on which to cross-validate.  By default uses the Primary.
        cv_splitter : CrossValidationSplitter object
            Cross-validation scheme to use.
        cv_implementation : CrossValidationImplementation object
            Cross-validation implementation to use.
        verbose : bool
            Whether to show fold times and metrics

        Returns
        -------
        pd.DataFrame
            Cross-validated metrics.  Contains the following columns:

            * model_name: name of the model
            * metric_name: name of the metric
            * fold: fold number
            * metric_value: value of the metric for the fold

        Notes
        -----

        You can use pipedown.visualization.plot_metrics to plot the dataframe
        returned from this method.

        .. code-block:: python3

            from pipedown.visualization import plot_metrics
            from pipedown.pipeline import Pipeline

            my_pipeline = # your pipeline

            metrics = my_pipeline.cv_metric()

            plot_metrics(metrics)

        """

        # Default is to run all metrics in the pipeline
        if isinstance(outputs, str):
            outputs = [outputs]
        if len(outputs) == 0:
            outputs = [n.name for n in self.get_nodes(Metric)]

        # Run the pipeline up to the node to cross validate on
        X, y, _ = self.run_to_cv_node(inputs, cv_on)

        # Run the cross-validation
        metrics = cv_implementation.run(
            self, cv_on, X, y, outputs, cv_splitter, verbose=verbose
        )

        # Convert the metrics into a dataframe
        if len(outputs) == 1:  # only a single output, metrics is a list
            metrics = [{outputs[0]: m} for m in metrics]
        metric_list = []
        for output in outputs:
            m_node = self.get_node(output)
            for i, metric_set in enumerate(metrics):
                metric_list.append(
                    {
                        "model_name": m_node.get_parents()[0].name,
                        "metric_name": m_node.get_metric_name(),
                        "fold": i,
                        "metric_value": metric_set[output],
                    }
                )

        # Return the metrics as a dataframe
        return pd.DataFrame.from_records(metrics)

    def run_to_cv_node(self, inputs, cv_on):
        """Run the pipeline up to the node to cross validate on"""
        if cv_on is None:
            cv_on = self.get_primary().name
        X, y = self.fit_run(inputs=inputs, outputs=[cv_on])
        original_index = X.index
        X = X.reset_index(inplace=True, drop=True)
        y = y.reset_index(inplace=True, drop=True)
        return X, y, original_index

    def save(self, filename: str):
        """Serialize the entire pipeline"""
        save_dag(self, filename)

    def get_html(self):
        """Get html for the dashboard displaying the pipeline"""
        return get_dag_viewer_html(self)

    def save_html(self, filename: str):
        """Save an html file with the dashboard displaying the pipeline"""
        with open(filename, "w") as f:
            f.write(self.get_html())
