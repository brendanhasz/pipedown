import pytest
import pandas as pd

from pipedown.nodes.base.input import Input


def test_input():

    node = Input("my_input")

    assert node.name == "my_input"

    with pytest.raises(ValueError):
        node.run({"a": 1, "b": 2}, format="lala")

    # dataframe, setting format
    dfo = node.run(pd.DataFrame.from_dict({"a": [1, 2, 3], "b": ['c', 'd', 'e']}), format="dataframe")
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 3
    assert dfo.shape[1] == 2
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[0, 1] == 'c'
    assert dfo.iloc[1, 1] == 'd'
    assert dfo.iloc[2, 1] == 'e'

    # dataframe, using auto
    dfo = node.run(pd.DataFrame.from_dict({"a": [1, 2, 3], "b": ['c', 'd', 'e']}))
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 3
    assert dfo.shape[1] == 2
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[0, 1] == 'c'
    assert dfo.iloc[1, 1] == 'd'
    assert dfo.iloc[2, 1] == 'e'

    # series, setting format
    dfo = node.run(pd.Series({"a": 1, "b": 'c', 'd': 1.0}), format="series")
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 1
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[0, 1] == 'c'
    assert dfo.iloc[0, 2] == 1.0

    # series, using auto
    dfo = node.run(pd.Series({"a": 1, "b": 'c', 'd': 1.0}))
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 1
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[0, 1] == 'c'
    assert dfo.iloc[0, 2] == 1.0

    # list of dicts, setting format
    dfo = node.run([{'a': 1, 'b': 'lala', 'd': 1.0}, {'a': 2, 'b': "lolo", 'd': 2.0}], format="rows")
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 2
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[0, 1] == 'lala'
    assert dfo.iloc[0, 2] == 1.0
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[1, 1] == 'lolo'
    assert dfo.iloc[1, 2] == 2.0

    # list of dicts, using auto
    dfo = node.run([{'a': 1, 'b': 'lala', 'd': 1.0}, {'a': 2, 'b': "lolo", 'd': 2.0}])
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 2
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[0, 1] == 'lala'
    assert dfo.iloc[0, 2] == 1.0
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[1, 1] == 'lolo'
    assert dfo.iloc[1, 2] == 2.0

    # dict of lists, setting format
    dfo = node.run({"a": [1, 2, 3, 4], "b": ["lala", "lolo", 'haha', 'hoho'], 'd': [5.0, 6.0, 7.0, 8.0]}, format="columns")
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 4
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[3, 0] == 4
    assert dfo.iloc[0, 1] == 'lala'
    assert dfo.iloc[1, 1] == "lolo"
    assert dfo.iloc[2, 1] == 'haha'
    assert dfo.iloc[3, 1] == 'hoho'
    assert dfo.iloc[0, 2] == 5.0
    assert dfo.iloc[1, 2] == 6.0
    assert dfo.iloc[2, 2] == 7.0
    assert dfo.iloc[3, 2] == 8.0

    # dict of lists, using auto
    dfo = node.run({"a": [1, 2, 3, 4], "b": ["lala", "lolo", 'haha', 'hoho'], 'd': [5.0, 6.0, 7.0, 8.0]})
    assert isinstance(dfo, pd.DataFrame)
    assert dfo.shape[0] == 4
    assert dfo.shape[1] == 3
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 3
    assert dfo.iloc[3, 0] == 4
    assert dfo.iloc[0, 1] == 'lala'
    assert dfo.iloc[1, 1] == "lolo"
    assert dfo.iloc[2, 1] == 'haha'
    assert dfo.iloc[3, 1] == 'hoho'
    assert dfo.iloc[0, 2] == 5.0
    assert dfo.iloc[1, 2] == 6.0
    assert dfo.iloc[2, 2] == 7.0
    assert dfo.iloc[3, 2] == 8.0
