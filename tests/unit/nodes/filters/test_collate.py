import pandas as pd

from pipedown.nodes.filters.collate import Collate


def test_collate():

    df = pd.DataFrame()
    df["a"] = [1, 2, 3, 4]
    df["b"] = ["a", "b", "c", "d"]
    df["c"] = [5.0, 6.0, 7.0, 8.0]

    collate = Collate()
    xo, yo = collate.run(
        (df[["a", "b"]].iloc[[1, 3]], df["c"].iloc[[1, 3]]),
        (df[["a", "b"]].iloc[[0, 2]], df["c"].iloc[[0, 2]]),
    )

    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 4
    assert xo.shape[1] == 2
    assert xo.iloc[0, 0] == 1
    assert xo.iloc[1, 0] == 2
    assert xo.iloc[2, 0] == 3
    assert xo.iloc[3, 0] == 4
    assert xo.iloc[0, 1] == "a"
    assert xo.iloc[1, 1] == "b"
    assert xo.iloc[2, 1] == "c"
    assert xo.iloc[3, 1] == "d"
    assert isinstance(yo, pd.Series)
    assert yo.shape[0] == 4
    assert yo.iloc[0] == 5.0
    assert yo.iloc[1] == 6.0
    assert yo.iloc[2] == 7.0
    assert yo.iloc[3] == 8.0
