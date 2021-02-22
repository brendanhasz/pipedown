import numpy as np
import pandas as pd

from pipedown.nodes.base.metric import Metric


class MedianAbsolutePercentageError(Metric):
    def run(self, y_pred: pd.Series, y_true: pd.Series):
        return 100 * np.nanmedian(np.abs(y_pred - y_true) / y_true)

    def get_metric_name(self):
        return "median_absolute_percentage_error"
