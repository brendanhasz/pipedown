import pandas as pd

from pipedown.nodes.transforms.rename_fields import RenameFields


def test_rename_fields():

    df = pd.DataFrame()
    df["thing1"] = [1, 2, 3, 4, 5]
    df["thing_red"] = ["a", "b", "c", "d", "e"]
    df["one_more_thing"] = ["a", "b", "c", "d", "e"]

    rf = RenameFields({"thing1": "thing2", "thing_red": "thing_blue"})
    dfo = rf.run(df)

    assert isinstance(dfo, pd.DataFrame)
    assert "thing1" not in dfo
    assert "thing_red" not in dfo
    assert "thing2" in dfo
    assert "thing_blue" in dfo
    assert "one_more_thing" in dfo
