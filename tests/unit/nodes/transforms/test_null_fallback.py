import numpy as np
import pandas as pd

from pipedown.nodes.transforms.null_fallback import NullFallback


def test_null_fallback():
    
    df = pd.DataFrame()
    df['a'] = [1, 2, np.nan, 3]
    df['b'] = [4, 5, 6, np.nan]
    df['c'] = [20, 30, 40, 50]
    
    nf = NullFallback([('a', 'b'), ('b', 'c')])
    dfo = nf.run(df, None)
    
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 6
    assert dfo.iloc[3, 0] == 3
    assert dfo.iloc[0, 1] == 4
    assert dfo.iloc[1, 1] == 5
    assert dfo.iloc[2, 1] == 6
    assert dfo.iloc[3, 1] == 50
    
def test_null_fallback_double_replacement():
    
    df = pd.DataFrame()
    df['a'] = [1, 2, np.nan, 3]
    df['b'] = [4, 5, np.nan, np.nan]
    df['c'] = [20, 30, 40, 50]
    
    nf = ReplaceNull([('a', 'b'), ('b', 'c')], n=2)
    dfo = nf.run(df, None)
    
    assert dfo.iloc[0, 0] == 1
    assert dfo.iloc[1, 0] == 2
    assert dfo.iloc[2, 0] == 40
    assert dfo.iloc[3, 0] == 3
    assert dfo.iloc[0, 1] == 4
    assert dfo.iloc[1, 1] == 5
    assert dfo.iloc[2, 1] == 40
    assert dfo.iloc[3, 1] == 50
