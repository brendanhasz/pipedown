from datetime import datetime

import pandas as pd

from pipedown.cross_validation.splitters import TimeBinSplitter


def test_time_bin_splitter():

    X = pd.DataFrame()
    X["a"] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    X["b"] = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    X["time"] = [
        datetime(2021, 3, 1),
        datetime(2021, 3, 2),
        datetime(2021, 3, 3),
        datetime(2021, 3, 4),
        datetime(2021, 3, 5),
        datetime(2021, 3, 6),
        datetime(2021, 3, 8),
        datetime(2021, 3, 9),
        datetime(2021, 3, 11),
        datetime(2021, 3, 12),
        datetime(2021, 3, 14),
        datetime(2021, 3, 15),
    ]
    y = pd.Series([21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32])

    time_bins = [
        (
            datetime(2021, 2, 25),
            datetime(2021, 3, 7),
            datetime(2021, 3, 7),
            datetime(2021, 3, 10),
        ),
        (
            datetime(2021, 2, 25),
            datetime(2021, 3, 10),
            datetime(2021, 3, 10),
            datetime(2021, 3, 13),
        ),
        (
            datetime(2021, 2, 25),
            datetime(2021, 3, 13),
            datetime(2021, 3, 13),
            datetime(2021, 3, 16),
        ),
    ]
    tbs = TimeBinSplitter(time_col="time", time_bins=time_bins)

    # this shouldn't do anything, but shouldn't crash either
    tbs.setup(X, y)

    assert tbs.get_n_folds() == 3

    x_train, y_train, x_val, y_val = tbs.get_fold(X, y, 0)
    assert isinstance(x_train, pd.DataFrame)
    assert isinstance(x_val, pd.DataFrame)
    assert isinstance(y_train, pd.Series)
    assert isinstance(y_val, pd.Series)
    assert x_train.shape[0] == 6
    assert x_train.shape[1] == 3
    assert x_val.shape[0] == 2
    assert x_val.shape[1] == 3
    assert y_train.shape[0] == 6
    assert y_val.shape[0] == 2
    assert x_train["a"].iloc[0] == 1
    assert x_train["a"].iloc[1] == 2
    assert x_train["a"].iloc[2] == 3
    assert x_train["a"].iloc[3] == 4
    assert x_train["a"].iloc[4] == 5
    assert x_train["a"].iloc[5] == 6
    assert x_train["b"].iloc[0] == 11
    assert x_train["b"].iloc[1] == 12
    assert x_train["b"].iloc[2] == 13
    assert x_train["b"].iloc[3] == 14
    assert x_train["b"].iloc[4] == 15
    assert x_train["b"].iloc[5] == 16
    assert y_train.iloc[0] == 21
    assert y_train.iloc[1] == 22
    assert y_train.iloc[2] == 23
    assert y_train.iloc[3] == 24
    assert y_train.iloc[4] == 25
    assert y_train.iloc[5] == 26
    assert x_val["a"].iloc[0] == 7
    assert x_val["a"].iloc[1] == 8
    assert x_val["b"].iloc[0] == 17
    assert x_val["b"].iloc[1] == 18
    assert y_val.iloc[0] == 27
    assert y_val.iloc[1] == 28

    x_train, y_train, x_val, y_val = tbs.get_fold(X, y, 1)
    assert isinstance(x_train, pd.DataFrame)
    assert isinstance(x_val, pd.DataFrame)
    assert isinstance(y_train, pd.Series)
    assert isinstance(y_val, pd.Series)
    assert x_train.shape[0] == 8
    assert x_train.shape[1] == 3
    assert x_val.shape[0] == 2
    assert x_val.shape[1] == 3
    assert y_train.shape[0] == 8
    assert y_val.shape[0] == 2
    assert x_train["a"].iloc[0] == 1
    assert x_train["a"].iloc[1] == 2
    assert x_train["a"].iloc[2] == 3
    assert x_train["a"].iloc[3] == 4
    assert x_train["a"].iloc[4] == 5
    assert x_train["a"].iloc[5] == 6
    assert x_train["a"].iloc[6] == 7
    assert x_train["a"].iloc[7] == 8
    assert x_train["b"].iloc[0] == 11
    assert x_train["b"].iloc[1] == 12
    assert x_train["b"].iloc[2] == 13
    assert x_train["b"].iloc[3] == 14
    assert x_train["b"].iloc[4] == 15
    assert x_train["b"].iloc[5] == 16
    assert x_train["b"].iloc[6] == 17
    assert x_train["b"].iloc[7] == 18
    assert y_train.iloc[0] == 21
    assert y_train.iloc[1] == 22
    assert y_train.iloc[2] == 23
    assert y_train.iloc[3] == 24
    assert y_train.iloc[4] == 25
    assert y_train.iloc[5] == 26
    assert y_train.iloc[6] == 27
    assert y_train.iloc[7] == 28
    assert x_val["a"].iloc[0] == 9
    assert x_val["a"].iloc[1] == 10
    assert x_val["b"].iloc[0] == 19
    assert x_val["b"].iloc[1] == 20
    assert y_val.iloc[0] == 29
    assert y_val.iloc[1] == 30

    x_train, y_train, x_val, y_val = tbs.get_fold(X, y, 2)
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
    assert x_train["a"].iloc[0] == 1
    assert x_train["a"].iloc[1] == 2
    assert x_train["a"].iloc[2] == 3
    assert x_train["a"].iloc[3] == 4
    assert x_train["a"].iloc[4] == 5
    assert x_train["a"].iloc[5] == 6
    assert x_train["a"].iloc[6] == 7
    assert x_train["a"].iloc[7] == 8
    assert x_train["a"].iloc[8] == 9
    assert x_train["a"].iloc[9] == 10
    assert x_train["b"].iloc[0] == 11
    assert x_train["b"].iloc[1] == 12
    assert x_train["b"].iloc[2] == 13
    assert x_train["b"].iloc[3] == 14
    assert x_train["b"].iloc[4] == 15
    assert x_train["b"].iloc[5] == 16
    assert x_train["b"].iloc[6] == 17
    assert x_train["b"].iloc[7] == 18
    assert x_train["b"].iloc[8] == 19
    assert x_train["b"].iloc[9] == 20
    assert y_train.iloc[0] == 21
    assert y_train.iloc[1] == 22
    assert y_train.iloc[2] == 23
    assert y_train.iloc[3] == 24
    assert y_train.iloc[4] == 25
    assert y_train.iloc[5] == 26
    assert y_train.iloc[6] == 27
    assert y_train.iloc[7] == 28
    assert y_train.iloc[8] == 29
    assert y_train.iloc[9] == 30
    assert x_val["a"].iloc[0] == 11
    assert x_val["a"].iloc[1] == 12
    assert x_val["b"].iloc[0] == 21
    assert x_val["b"].iloc[1] == 22
    assert y_val.iloc[0] == 31
    assert y_val.iloc[1] == 32
