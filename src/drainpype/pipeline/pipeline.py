from abc import ABC, abstractmethod
from typing import Tuple

from drainpype.cross_validation import RandomCrossValidator

class Pipeline(ABC):
    """Abstract base class for a Pipeline"""

    @abstractmethod
    def nodes(self):
        """Initialize the components used in the pipeline"""
        pass

    @abstractmethod
    def pipeline(self):
        """Define the connections between pipeline components"""
        pass

    def fit(self, inputs=None, outputs=None): 
        """Fit part of or the whole pipeline"""
        # TODO

    def run(self, inputs=None, outputs=None): 
        """Run part of or the whole pipeline"""
        # TODO

    def cv_predict(self, inputs=None, outputs=None, cross_validator=RandomCrossValidator()): 
        """Make cross-validated predictions using the pipeline

        Parameters
        ----------
        inputs : None or pd.DataFrame
            Input to use as the primary data.  By default will just run the
            whole pipeline up to the model node(s) specified in `outputs`.
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
        
        You can use drainpype.visualization.plot_metrics to plot the dataframe
        returned from this method.

        .. code-block:: python3

            from drainpype.visualization import plot_metrics
            from drainpype.pipeline import Pipeline

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
