import pandas as pd

from pipedown.nodes.base.primary import Primary

def test_primary():

    df = pd.DataFrame()
    df['a'] = [1, 2, 3, 4]
    df['b'] = ['a', 'b', 'c', 'd']
    df['d'] = [1.0, 2.0, 3.0, 4.0]
    df['e'] = [5.0, 6.0, 7.0, 8.0]

    node = Primary('primary', ['a', 'b', 'd'], 'e')

    assert node.name == "primary"

    x, y = node.run(df, mode="train")
    assert isinstance(x, pd.DataFrame)
    assert x.shape[0] == 4
    assert x.shape[1] == 3
    assert 'a' in x
    assert 'b' in x
    assert 'd' in x
    assert x.iloc[0, 0] == 1
    assert x.iloc[1, 0] == 2
    assert x.iloc[2, 0] == 3
    assert x.iloc[3, 0] == 4
    assert x.iloc[0, 1] == 'a'
    assert x.iloc[1, 1] == 'b'
    assert x.iloc[2, 1] == 'c'
    assert x.iloc[3, 1] == 'd'
    assert x.iloc[0, 2] == 1.0
    assert x.iloc[1, 2] == 2.0
    assert x.iloc[2, 2] == 3.0
    assert x.iloc[3, 2] == 4.0
    assert isinstance(y, pd.Series)
    assert y.shape[0] == 4
    assert y.iloc[0] == 5.0
    assert y.iloc[1] == 6.0
    assert y.iloc[2] == 7.0
    assert y.iloc[3] == 8.0

    x, y = node.run(df[['a', 'b', 'd']], mode="test")
    assert isinstance(x, pd.DataFrame)
    assert x.shape[0] == 4
    assert x.shape[1] == 3
    assert 'a' in x
    assert 'b' in x
    assert 'd' in x
    assert x.iloc[0, 0] == 1
    assert x.iloc[1, 0] == 2
    assert x.iloc[2, 0] == 3
    assert x.iloc[3, 0] == 4
    assert x.iloc[0, 1] == 'a'
    assert x.iloc[1, 1] == 'b'
    assert x.iloc[2, 1] == 'c'
    assert x.iloc[3, 1] == 'd'
    assert x.iloc[0, 2] == 1.0
    assert x.iloc[1, 2] == 2.0
    assert x.iloc[2, 2] == 3.0
    assert x.iloc[3, 2] == 4.0
    assert y is None
