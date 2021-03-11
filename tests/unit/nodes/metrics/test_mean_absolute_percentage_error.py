import pandas as pd

from pipedown.nodes.metrics import MeanAbsolutePercentageError


def test_mean_absolute_percentage_error(is_close):

    y_pred = pd.Series([1.0, 2.0, 3.0, 4.0])
    y_true = pd.Series([1.0, 3.0, 5.0, 7.0])

    mape = MeanAbsolutePercentageError()
    v = mape.run(y_pred, y_true)
    assert isinstance(v, float)
    assert is_close(v, 100 * (0 + 1 / 3 + 2 / 5 + 3 / 7) / 4)

    assert mape.get_metric_name() == "mean_absolute_percentage_error"
