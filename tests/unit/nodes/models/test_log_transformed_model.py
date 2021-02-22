import numpy as np
import pandas as pd

from pipedown.nodes.models.catboost_regressor_model import (
    CatBoostRegressorModel,
)
from pipedown.nodes.models.log_transformed_model import LogTransformedModel


def test_log_transformed_model():

    df = pd.DataFrame()
    df["a"] = np.random.randn(10)
    df["b"] = np.random.randn(10)
    df["c"] = np.exp(np.random.randn() * df["a"] + np.random.randn() * df["b"])

    crm = LogTransformedModel(CatBoostRegressorModel(verbose=False, thread_count=1))

    # Fit it
    crm.fit(df[["a", "b"]], df["c"])

    # Run in validation mode
    y_pred, y_true = crm.run(df[["a", "b"]].iloc[:3, :], df["c"].iloc[:3])
    assert isinstance(y_pred, pd.Series)
    assert y_pred.shape[0] == 3
    assert isinstance(y_true, pd.Series)
    assert y_true.shape[0] == 3

    # In test mode
    y_pred, y_true = crm.run(df[["a", "b"]].iloc[:4, :], None)
    assert y_true is None
    assert isinstance(y_pred, pd.Series)
    assert y_pred.shape[0] == 4
