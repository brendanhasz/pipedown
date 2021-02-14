from abc import ABC, abstractmethod
from typing import Tuple, List, Any

from pipedown.nodes.base.node import Node
from pipedown.nodes.base.primary import Primary
from pipedown.cross_validation import RandomCrossValidator

class Pipeline(Node):
    """Abstract base class for a Pipeline"""

    @abstractmethod
    def nodes(self):
        """Initialize the components used in the pipeline"""
        pass

    @abstractmethod
    def pipeline(self):
        """Define the connections between pipeline components"""
        pass

    def fit(self, inputs: Dict[str, Any]={}, outputs: Union[str, List[str]]=[]):
        """Fit part of or the whole pipeline"""
        self._run_dag(inputs, outputs, "train")

    def run(self, inputs: Dict[str, Any]={}, outputs: Union[str, List[str]]=[]):
        """Run part of or the whole pipeline"""
        return self._run_dag(inputs, outputs, "test")

    def _run_dag(self, inputs: Dict[str, Any], outputs: Union[str, List[str]], mode):
        """Run the dag between inputs and outputs"""

        # Assign parents + children for training mode
        self._materialize_dag(mode=mode)

        # Default output nodes are all nodes without children
        if len(outputs) == 0:
            outputs = [
                n.name for n in self.get_nodes() if n.num_children() == 0
            ]

        # To store cached outputs
        cached_outputs = {}
        output_data = {}

        def fit_node(node_inputs, node, mode):
            if node_inputs is None:
                if mode == "train":
                    node.fit()
                return node.run()
            elif isinstance(node_inputs, tuple):
                if mode == "train":
                    node.fit(*node_inputs)
                return node.run(*node_inputs)
            else:
                if mode == "train":
                    node.fit(node_inputs)
                return node.run(node_inputs)

        # Run each node
        for node in self._get_dag_eval_order(inputs, outputs):

            # Fit and run the node
            if node.num_parents() == 0:
                node_outputs = fit_node(inputs.get(node.name), node)
            elif node.num_parents() == 1:
                parent_name = node.get_parents()[0].name
                if parent_name in cached_outputs:
                    node_outputs = fit_node(deepcopy(cached_outputs[parent_name]), node)
                else:  # only child / single parent -> we ran this last step
                    node_outputs = fit_node(node_outputs, node)
            else:  # >1 parent, all of whose outputs will have been cached
                node_outputs = fit_node(
                    (deepcopy(cached_outputs[p.name]) for p in node.get_parents()),
                    node
                )

            # Cache this node's outputs if it has multiple children
            # or if any of its children have multiple parents
            # TODO: there's definitely a more efficient way to be doing this...
            # should delete data as soon as it won't be needed by further nodes
            # and way you've currently got it set up there's an extra copy made
            if node.num_children() > 1 or any(c.num_parents() > 1 for c in node.get_children()):
                cached_outputs[node.name] = deepcopy(node_outputs)

            # And store the output if this node is an output node
            if node.name in outputs:
                output_data[node_name] = node_outputs

        # Return output data
        if isinstance(outputs, str):
            return output_data[outputs]
        else:
            return output_data


    def _get_dag_eval_order(
        self,
        inputs: Dict[str, Any],
        outputs: List[str],
    ):
        """Get a list of nodes in the subset of the DAG connecting the inputs
        and outputs, in reverse post-order.
        """

        visited = set()
        visit_order = []

        def dfs_walk(node):
            if node.name not in inputs:  # truncate the walk at inputs
                for parent in node.get_parents():
                    if parent not in visited:
                        dfs_walk(parent)
            visited.add(node)
            visit_order.append(node)

        for node in self.get_nodes():
            if node.name in outputs:
                dfs_walk(node)


    def _materialize_dag(self, mode: str):
        """Assign doubly-linked edges depending on mode"""
        for node in self.get_nodes():
            node.reset_children()
        for node in self.get_nodes():
            if isinstance(node, Primary):
                if mode == "train":
                    node.set_parents(node.get_train_parent())
                    node.get_train_parent().add_children(node)
                else:
                    node.set_parents(node.get_test_parent())
                    node.get_test_parent().add_children(node)
            else:
                for parent in node.get_parents():
                    parent.add_children(node)


    def get_nodes(self) -> List[Node]:
        """Get a list of all nodes contained in this pipeline"""
        return [n for n in vars(self).values() if isinstance(n, Node)]

    def get_primary(self) -> Node:
        """Get the primary node if it exists in the DAG"""
        primaries = [p for p in self.get_nodes() if isinstance(p, Primary)]
        if len(primaries) < 1:
            return None
        elif len(primaries) == 1:
            return primaries[0]
        else:
            raise RuntimeError(
                "There are multiple Primary data sources in your DAG!"
            )

    def cv_predict(self, inputs=None, outputs=None, cross_validator=RandomCrossValidator()):
        """Make cross-validated predictions using the pipeline

        Parameters
        ----------
        inputs : None or pd.DataFrame
            Input to use, if any.  By default will just run the whole pipeline
            up to the model node(s) specified in `outputs`.
        outputs : List[str]
            List of names of the model nodes from which to get predictions.
        cross_validator : CrossValidator object
            Cross-validation scheme to use.

        Returns
        -------
        pd.DataFrame
            Cross-validated predictions.  Contains columns with the true target
            values (column `y_true`), and then one column for each of the
            models in `outputs` list, with that model's predictions.
        """

        # Run the pipeline up to the Primary
        df = self.run(outputs=[self.get_primary().name])

        # Run the cross-validation
        # TODO

    def cv_metric(self, inputs=None, outputs=None, cross_validator=RandomCrossValidator()):
        """Compute metric(s) from cross-validated predictions

        Parameters
        ----------
        inputs : None or pd.DataFrame
            Input to use as the primary data.  By default will just run the
            whole pipeline up to the metric node(s) specified in `outputs`.
        outputs : List[str]
            List of names of the metric nodes to evaluate.
        cross_validator : CrossValidator object
            Cross-validation scheme to use.

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
        # TODO

    def save(self, filename: str):
        """Serialize the entire pipeline"""
        save_pipeline(self, filename)

    def get_html(self):
        """Get html for the dashboard displaying the pipeline"""
        # TODO

    def save_html(self, filename: str):
        """Save an html file with the dashboard displaying the pipeline"""
        with open(filename, "w") as f:
            f.write(self.get_html())
