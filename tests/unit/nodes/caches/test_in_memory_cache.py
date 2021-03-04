import pandas as pd

from pipedown.nodes.caches import InMemoryCache


def test_in_memory_cache():

    imc = InMemoryCache()

    df = pd.DataFrame()
    df["a"] = [1, 2, 3]
    df["b"] = ["a", "b", "c"]

    assert not imc.is_cached()

    imc.fit(df)

    assert imc.is_cached()

    # Fit should run fine when passed no args
    imc.fit(df)

    dfo = imc.run(None)

    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 3
    assert dfo.shape[1] == 2
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[0, 1] == "a"
    assert dfo.iloc[1, 1] == "b"
    assert dfo.iloc[2, 1] == "c"

    # Should also work if passed no args
    dfo = imc.run()
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 3
    assert dfo.shape[1] == 2
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[0, 1] == "a"
    assert dfo.iloc[1, 1] == "b"
    assert dfo.iloc[2, 1] == "c"

    imc.clear_cache()

    df2 = pd.DataFrame()
    df2["c"] = [10, 20, 30, 40]
    df2["d"] = ["aa", "bb", "cc", "dd"]

    assert not imc.is_cached()

    dfo = imc.run(df2)

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
