
from pipedown.nodes.base.metric import Metric

class MeanSquaredError(Metric):

    def run(self, y_pred: pd.Series, y_true: pd.Series):
        return np.mean(np.square(y_pred - y_true))

    def get_metric_name(self):
        return "mean_squared_error"
    
