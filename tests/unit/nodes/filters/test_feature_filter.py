import pandas as pd

from pipedown.nodes.filters.feature_filter import FeatureFilter

def test_feature_filter():

    X = pd.DataFrame()
    X['a'] = [1, 2, 3, 4]
    X['b'] = ['a', 'b', 'c', 'd']
    X['c'] = [5.0, 6.0, 7.0, 8.0]
    y = pd.Series([10, 11, 12, 13])

    feature_filter = FeatureFilter("name", ['a', 'c'])
    xo, yo = feature_filter.run(X, y)

    assert isinstance(xo, pd.DataFrame)
    assert xo.shape[0] == 4
    assert xo.shape[1] == 2
    assert 'a' in xo
    assert 'b' not in xo
    assert 'c' in xo
    assert xo.iloc[0, 0] == 1
    assert xo.iloc[1, 0] == 2
    assert xo.iloc[2, 0] == 3
    assert xo.iloc[3, 0] == 4
    assert xo.iloc[0, 1] == 5.0
    assert xo.iloc[1, 1] == 6.0
    assert xo.iloc[2, 1] == 7.0
    assert xo.iloc[3, 1] == 8.0

    assert isinstance(yo, pd.Series)
    assert yo.shape[0] == 4
    assert yo.iloc[0] == 10
    assert yo.iloc[1] == 11
    assert yo.iloc[2] == 12
    assert yo.iloc[3] == 13
