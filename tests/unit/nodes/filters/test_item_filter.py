import pandas as pd

from pipedown.nodes.filters.item_filter import ItemFilter

def test_item_filter():

    X = pd.DataFrame()
    X['a'] = [1, 2, 3, 4]
    X['b'] = ['a', 'b', 'c', 'c']
    X['c'] = [5.0, 6.0, 7.0, 8.0]
    y = pd.Series([10, 11, 12, 13])

    item_filter = ItemFilter(lambda df: (df['a']>1) & (df['c']<8))
    xo, yo = item_filter.run(X, y)
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 2
    assert xo.shape[1] == 3
    assert 'a' in xo
    assert 'b' in xo
    assert 'c' in xo
    assert xo.iloc[0, 0] == 2
    assert xo.iloc[1, 0] == 3
    assert xo.iloc[0, 1] == 'b'
    assert xo.iloc[1, 1] == 'c'
    assert xo.iloc[0, 2] == 6.0
    assert xo.iloc[1, 2] == 7.0
    assert isinstance(yo, pd.Series)
    assert yo.shape[0] == 2
    assert yo.iloc[0] == 11
    assert yo.iloc[1] == 12

    item_filter = ItemFilter(lambda df: df['b']=='c')
    xo, yo = item_filter.run(X, y)
    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 2
    assert xo.shape[1] == 3
    assert 'a' in xo
    assert 'b' in xo
    assert 'c' in xo
    assert xo.iloc[0, 0] == 3
    assert xo.iloc[1, 0] == 4
    assert xo.iloc[0, 1] == 'c'
    assert xo.iloc[1, 1] == 'c'
    assert xo.iloc[0, 2] == 7.0
    assert xo.iloc[1, 2] == 8.0
    assert isinstance(yo, pd.Series)
    assert yo.shape[0] == 2
    assert yo.iloc[0] == 12
    assert yo.iloc[1] == 13
