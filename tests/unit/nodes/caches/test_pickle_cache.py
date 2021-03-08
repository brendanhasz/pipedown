import os

import pandas as pd

from pipedown.nodes.caches import PickleCache


def test_pickle_cache():

    if os.path.exists("test-filename.pkl"):
        os.remove("test-filename.pkl")

    pc = PickleCache("test-filename.pkl")

    df = pd.DataFrame()
    df["a"] = [1, 2, 3]
    df["b"] = ["a", "b", "c"]
    df["c"] = [
        [(1, 2), (3, 4), (5, 6)],
        [(11, 12), (13, 14), (15, 16)],
        [(21, 22), (23, 24), (25, 26)],
    ]

    assert not pc.is_cached()

    pc.fit(df)

    assert os.path.exists("test-filename.pkl")
    assert os.path.isfile("test-filename.pkl")
    assert pc.is_cached()

    # Fit should run fine when passed no args
    pc.fit()

    dfo = pc.run(None)

    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 3
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[0, 1] == "a"
    assert dfo.iloc[1, 1] == "b"
    assert dfo.iloc[2, 1] == "c"
    for i in range(3):
        assert isinstance(dfo.iloc[i, 2], list)
        for j in range(3):
            assert isinstance(dfo.iloc[i, 2][j], tuple)
    assert dfo.iloc[0, 2][0][0] == 1
    assert dfo.iloc[0, 2][0][1] == 2
    assert dfo.iloc[0, 2][1][0] == 3
    assert dfo.iloc[0, 2][1][1] == 4
    assert dfo.iloc[0, 2][2][0] == 5
    assert dfo.iloc[0, 2][2][1] == 6
    assert dfo.iloc[1, 2][0][0] == 11
    assert dfo.iloc[1, 2][0][1] == 12
    assert dfo.iloc[1, 2][1][0] == 13
    assert dfo.iloc[1, 2][1][1] == 14
    assert dfo.iloc[1, 2][2][0] == 15
    assert dfo.iloc[1, 2][2][1] == 16
    assert dfo.iloc[2, 2][0][0] == 21
    assert dfo.iloc[2, 2][0][1] == 22
    assert dfo.iloc[2, 2][1][0] == 23
    assert dfo.iloc[2, 2][1][1] == 24
    assert dfo.iloc[2, 2][2][0] == 25
    assert dfo.iloc[2, 2][2][1] == 26

    # Should also work if passed no args
    dfo = pc.run()
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 3
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[0, 1] == "a"
    assert dfo.iloc[1, 1] == "b"
    assert dfo.iloc[2, 1] == "c"
    for i in range(3):
        assert isinstance(dfo.iloc[i, 2], list)
        for j in range(3):
            assert isinstance(dfo.iloc[i, 2][j], tuple)
    assert dfo.iloc[0, 2][0][0] == 1
    assert dfo.iloc[0, 2][0][1] == 2
    assert dfo.iloc[0, 2][1][0] == 3
    assert dfo.iloc[0, 2][1][1] == 4
    assert dfo.iloc[0, 2][2][0] == 5
    assert dfo.iloc[0, 2][2][1] == 6
    assert dfo.iloc[1, 2][0][0] == 11
    assert dfo.iloc[1, 2][0][1] == 12
    assert dfo.iloc[1, 2][1][0] == 13
    assert dfo.iloc[1, 2][1][1] == 14
    assert dfo.iloc[1, 2][2][0] == 15
    assert dfo.iloc[1, 2][2][1] == 16
    assert dfo.iloc[2, 2][0][0] == 21
    assert dfo.iloc[2, 2][0][1] == 22
    assert dfo.iloc[2, 2][1][0] == 23
    assert dfo.iloc[2, 2][1][1] == 24
    assert dfo.iloc[2, 2][2][0] == 25
    assert dfo.iloc[2, 2][2][1] == 26

    pc.clear_cache()

    df2 = pd.DataFrame()
    df2["c"] = [10, 20, 30, 40]
    df2["d"] = ["aa", "bb", "cc", "dd"]
    y2 = pd.Series([11, 22, 33, 44])

    assert not os.path.exists("test-filename.pkl")
    assert not pc.is_cached()

    dfo, yo = pc.run(df2, y2)

    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 4
    assert dfo.shape[1] == 2
    assert dfo.iloc[0, 0] == 10
    assert dfo.iloc[1, 0] == 20
    assert dfo.iloc[2, 0] == 30
    assert dfo.iloc[3, 0] == 40
    assert dfo.iloc[0, 1] == "aa"
    assert dfo.iloc[1, 1] == "bb"
    assert dfo.iloc[2, 1] == "cc"
    assert dfo.iloc[3, 1] == "dd"
    assert isinstance(yo, pd.Series)
    assert yo.iloc[0] == 11
    assert yo.iloc[1] == 22
    assert yo.iloc[2] == 33
    assert yo.iloc[3] == 44
