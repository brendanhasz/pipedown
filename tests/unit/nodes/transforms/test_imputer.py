import numpy as np
import pandas as pd

from pipedown.nodes.transforms.imputer import Imputer


def test_imputer(is_close):

    df = pd.DataFrame()
    df["a"] = [1, 3, np.nan, 5]
    df["b"] = [5, 6, 7, 8]
    df["c"] = ["a", np.nan, "c", "c"]
    df["d"] = pd.Categorical(pd.Series([np.nan, "d", "d", "e"]))
    df["e"] = pd.Series([1.0, 2.0, np.nan, 3.0], dtype="Float64")
    df["f"] = pd.Series([1.0, 2.0, 3.0, 3.0], dtype="Float64")

    # By default should run for all columns with nan values
    imputer = Imputer()
    imputer.fit(df, None)
    dfo, y = imputer.run(df, None)
    assert isinstance(dfo, pd.DataFrame)
    assert y is None
    assert dfo.shape[0] == 4
    assert dfo.shape[1] == 6
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 3
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[3, 0] == 5
    assert dfo.iloc[0, 1] == 5
    assert dfo.iloc[1, 1] == 6
    assert dfo.iloc[2, 1] == 7
    assert dfo.iloc[3, 1] == 8
    assert dfo.iloc[0, 2] == "a"
    assert dfo.iloc[1, 2] == "c"
    assert dfo.iloc[2, 2] == "c"
    assert dfo.iloc[3, 2] == "c"
    assert dfo.iloc[0, 3] == "d"
    assert dfo.iloc[1, 3] == "d"
    assert dfo.iloc[2, 3] == "d"
    assert dfo.iloc[3, 3] == "e"
    assert is_close(dfo.iloc[0, 4], 1.0)
    assert is_close(dfo.iloc[1, 4], 2.0)
    assert is_close(dfo.iloc[2, 4], 2.0)
    assert is_close(dfo.iloc[3, 4], 3.0)
    assert is_close(dfo.iloc[0, 5], 1.0)
    assert is_close(dfo.iloc[1, 5], 2.0)
    assert is_close(dfo.iloc[2, 5], 3.0)
    assert is_close(dfo.iloc[3, 5], 3.0)
