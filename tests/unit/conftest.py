import pytest
import numpy as np


@pytest.fixture(scope='session')
def is_close(request):

    def is_close_fn(a, b, thresh=1e-3):
        return np.all(np.abs(a-b) < thresh)

    return is_close_fn
