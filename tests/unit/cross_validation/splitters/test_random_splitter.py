import numpy as np
import pandas as pd

from pipedown.cross_validation.splitters import RandomSplitter


def test_random_splitter():

    X = pd.DataFrame()
    X["a"] = np.random.randn(12)
    X["b"] = np.random.randn(12)
    y = pd.Series(np.random.randn(12))

    rs = RandomSplitter(n_folds=4, random_seed=11111)

    rs.setup(X, y)

    assert isinstance(rs.ix, np.ndarray)
    assert rs.ix.shape[0] == 12
    assert rs.ix.min() == 0
    assert rs.ix.max() == 11
    assert rs.n == 12
    assert rs.n_per_fold == 3
    assert rs.get_n_folds() == 4

    for i in range(4):
        x_train, y_train, x_val, y_val = rs.get_fold(X, y, i)
        assert isinstance(x_train, pd.DataFrame)
        assert isinstance(x_val, pd.DataFrame)
        assert isinstance(y_train, pd.Series)
        assert isinstance(y_val, pd.Series)
        assert x_train.shape[0] == 9
        assert x_train.shape[1] == 2
        assert x_val.shape[0] == 3
        assert x_val.shape[1] == 2
        assert y_train.shape[0] == 9
        assert y_val.shape[0] == 3


def test_random_splitter_uneven():

    X = pd.DataFrame()
    X["a"] = np.random.randn(12)
    X["b"] = np.random.randn(12)
    X["c"] = np.random.randn(12)
    y = pd.Series(np.random.randn(12))

    rs = RandomSplitter()

    rs.setup(X, y)

    assert isinstance(rs.ix, np.ndarray)
    assert rs.ix.shape[0] == 12
    assert rs.ix.min() == 0
    assert rs.ix.max() == 11
    assert rs.n == 12
    assert rs.n_per_fold == 2
    assert rs.get_n_folds() == 5

    for i in range(4):
        x_train, y_train, x_val, y_val = rs.get_fold(X, y, i)
        assert isinstance(x_train, pd.DataFrame)
        assert isinstance(x_val, pd.DataFrame)
        assert isinstance(y_train, pd.Series)
        assert isinstance(y_val, pd.Series)
        assert x_train.shape[0] == 10
        assert x_train.shape[1] == 3
        assert x_val.shape[0] == 2
        assert x_val.shape[1] == 3
        assert y_train.shape[0] == 10
        assert y_val.shape[0] == 2

    # Final fold should go through the end
    x_train, y_train, x_val, y_val = rs.get_fold(X, y, 4)
    assert isinstance(x_train, pd.DataFrame)
    assert isinstance(x_val, pd.DataFrame)
    assert isinstance(y_train, pd.Series)
    assert isinstance(y_val, pd.Series)
    assert x_train.shape[0] == 8
    assert x_train.shape[1] == 3
    assert x_val.shape[0] == 4
    assert x_val.shape[1] == 3
    assert y_train.shape[0] == 8
    assert y_val.shape[0] == 4
