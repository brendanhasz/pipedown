import pandas as pd

from pipedown.nodes.metrics import MedianAbsolutePercentageError


def test_median_absolute_percentage_error(is_close):

    y_pred = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    y_true = pd.Series([1.0, 3.0, 5.0, 7.0, 9.0])

    mape = MedianAbsolutePercentageError()
    v = mape.run(y_pred, y_true)
    assert isinstance(v, float)
    assert is_close(v, 40)

    assert mape.get_metric_name() == "median_absolute_percentage_error"
