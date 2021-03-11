import pandas as pd

from pipedown.nodes.metrics import MeanSquaredError


def test_mean_squared_error(is_close):

    y_pred = pd.Series([1.0, 2.0, 3.0, 4.0])
    y_true = pd.Series([1.0, 3.0, 5.0, 7.0])

    mse = MeanSquaredError()
    v = mse.run(y_pred, y_true)
    assert isinstance(v, float)
    assert is_close(v, 3.5)

    assert mse.get_metric_name() == "mean_squared_error"
