import numpy as np
import pandas as pd
import pytest

from pipedown.nodes.base.input import Input
from pipedown.nodes.base.node import Node
from pipedown.nodes.base.primary import Primary
from pipedown.pipeline.dag import get_dag_eval_order, run_dag


class MyNode(Node):
    def run(self, *args):
        return args


def test_dag_eval_order_linear():

    # a -> b -> c -> d
    a = MyNode("a")
    b = MyNode("b")
    c = MyNode("c")
    d = MyNode("d")
    b.set_parents(a)
    a.add_children(b)
    c.set_parents(b)
    b.add_children(c)
    d.set_parents(c)
    c.add_children(d)

    # the whole dag
    order = get_dag_eval_order("a", "d", [a, b, c, d])
    assert isinstance(order, list)
    assert len(order) == 4
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert order[3] is d

    # should stop at output node even if there are other nodes in the DAG
    order = get_dag_eval_order("a", "c", [a, b, c, d])
    assert isinstance(order, list)
    assert len(order) == 3
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert d not in order

    # should start at input node even if there are other nodes in the DAG
    order = get_dag_eval_order("b", "d", [a, b, c, d])
    assert isinstance(order, list)
    assert len(order) == 3
    assert a not in order
    assert order[0] is b
    assert order[1] is c
    assert order[2] is d


def test_dag_eval_order_divergence():

    # a -> b -> c -> d
    #            \
    #             -> e -> f
    a = MyNode("a")
    b = MyNode("b")
    c = MyNode("c")
    d = MyNode("d")
    e = MyNode("e")
    f = MyNode("f")
    b.set_parents(a)
    a.add_children(b)
    c.set_parents(b)
    b.add_children(c)
    d.set_parents(c)
    c.add_children(d)
    e.set_parents(c)
    c.add_children(e)
    f.set_parents(e)
    e.add_children(f)
    nodes = [a, b, c, d, e, f]

    # Running a->f should not run d
    order = get_dag_eval_order("a", "f", nodes)
    assert isinstance(order, list)
    assert len(order) == 5
    assert d not in order
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert order[3] is e
    assert order[4] is f

    # Running a->d should not run e or f
    order = get_dag_eval_order("a", "d", nodes)
    assert isinstance(order, list)
    assert len(order) == 4
    assert e not in order
    assert f not in order
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert order[3] is d

    # Running with multiple outputs
    order = get_dag_eval_order("a", ["d", "f"], nodes)
    assert isinstance(order, list)
    assert len(order) == 6
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert d in order
    assert e in order
    assert f in order
    assert order.index(d) > order.index(c)
    assert order.index(e) > order.index(c)
    assert order.index(f) > order.index(e)

    # Running with multiple outputs, but stopping at output
    order = get_dag_eval_order("a", ["d", "e"], nodes)
    assert isinstance(order, list)
    assert len(order) == 5
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert d in order
    assert e in order
    assert f not in order
    assert order.index(d) > order.index(c)
    assert order.index(e) > order.index(c)


def test_dag_eval_order_multi_divergence():

    # a -> b -> c -> d
    #            \
    #             -> e -> f
    #                 \
    #                  -> g
    a = MyNode("a")
    b = MyNode("b")
    c = MyNode("c")
    d = MyNode("d")
    e = MyNode("e")
    f = MyNode("f")
    g = MyNode("g")
    b.set_parents(a)
    a.add_children(b)
    c.set_parents(b)
    b.add_children(c)
    d.set_parents(c)
    c.add_children(d)
    e.set_parents(c)
    c.add_children(e)
    f.set_parents(e)
    e.add_children(f)
    g.set_parents(e)
    e.add_children(g)
    nodes = [a, b, c, d, e, f, g]

    # Running a->f should not run d or g
    order = get_dag_eval_order("a", "f", nodes)
    assert isinstance(order, list)
    assert len(order) == 5
    assert d not in order
    assert g not in order
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert order[3] is e
    assert order[4] is f

    # Running a->d should not run e, f, or g
    order = get_dag_eval_order("a", "d", nodes)
    assert isinstance(order, list)
    assert len(order) == 4
    assert e not in order
    assert f not in order
    assert g not in order
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert order[3] is d

    # Running with multiple outputs (should not run g)
    order = get_dag_eval_order("a", ["d", "f"], nodes)
    assert isinstance(order, list)
    assert len(order) == 6
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert d in order
    assert e in order
    assert f in order
    assert g not in order
    assert order.index(d) > order.index(c)
    assert order.index(e) > order.index(c)
    assert order.index(f) > order.index(e)

    # All the outputs
    order = get_dag_eval_order("a", ["d", "f", "g"], nodes)
    assert isinstance(order, list)
    assert len(order) == 7
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert d in order
    assert e in order
    assert f in order
    assert g in order
    assert order.index(d) > order.index(c)
    assert order.index(e) > order.index(c)
    assert order.index(f) > order.index(e)
    assert order.index(g) > order.index(e)

    # Running with multiple outputs, but stopping at output
    order = get_dag_eval_order("a", ["d", "e"], nodes)
    assert isinstance(order, list)
    assert len(order) == 5
    assert order[0] is a
    assert order[1] is b
    assert order[2] is c
    assert d in order
    assert e in order
    assert f not in order
    assert g not in order
    assert order.index(d) > order.index(c)
    assert order.index(e) > order.index(c)


def test_dag_eval_order_multi_convergence():

    # a -> b -> f -> g
    #       /
    # c -> d
    #  /
    # e
    a = MyNode("a")
    b = MyNode("b")
    c = MyNode("c")
    d = MyNode("d")
    e = MyNode("e")
    f = MyNode("f")
    g = MyNode("g")
    b.set_parents(a)
    a.add_children(b)
    d.set_parents([c, e])
    c.add_children(d)
    e.add_children(d)
    f.set_parents([b, d])
    d.add_children(f)
    b.add_children(f)
    g.set_parents(f)
    f.add_children(g)
    nodes = [a, b, c, d, e, f, g]

    # Running the whole dag
    order = get_dag_eval_order(["a", "c", "e"], "g", nodes)
    assert isinstance(order, list)
    assert len(order) == 7
    assert a in order
    assert b in order
    assert c in order
    assert d in order
    assert e in order
    assert f in order
    assert g in order
    assert order[-1] is g
    assert order[-2] is f
    assert order.index(f) > order.index(b)
    assert order.index(b) > order.index(a)
    assert order.index(f) > order.index(d)
    assert order.index(d) > order.index(c)
    assert order.index(d) > order.index(e)

    # Running up to d should not run a, b, f, or g
    order = get_dag_eval_order(["c", "e"], "d", nodes)
    assert isinstance(order, list)
    assert len(order) == 3
    assert a not in order
    assert b not in order
    assert f not in order
    assert g not in order
    assert order[-1] is d
    assert order.index(d) > order.index(c)
    assert order.index(d) > order.index(e)

    # Should run disconnected subgraphs if need be
    order = get_dag_eval_order(["a", "c", "e"], ["b", "d"], nodes)
    assert isinstance(order, list)
    assert len(order) == 5
    assert a in order
    assert b in order
    assert c in order
    assert d in order
    assert e in order
    assert f not in order
    assert g not in order
    assert order.index(b) > order.index(a)
    assert order.index(d) > order.index(c)
    assert order.index(d) > order.index(e)


def test_dag_eval_order_divergence_and_convergence():

    # a -> b -> f -> g
    #       /
    # c -> d -> h -> i -> k
    #  /         \    /
    # e           -> j -> m
    a = MyNode("a")
    b = MyNode("b")
    c = MyNode("c")
    d = MyNode("d")
    e = MyNode("e")
    f = MyNode("f")
    g = MyNode("g")
    h = MyNode("h")
    i = MyNode("i")
    j = MyNode("j")
    k = MyNode("k")
    m = MyNode("m")
    b.set_parents(a)
    a.add_children(b)
    d.set_parents([c, e])
    c.add_children(d)
    e.add_children(d)
    f.set_parents([b, d])
    d.add_children(f)
    b.add_children(f)
    g.set_parents(f)
    f.add_children(g)
    h.set_parents(d)
    d.add_children(h)
    i.set_parents(h)
    h.add_children([i, j])
    j.set_parents(h)
    k.set_parents([i, j])
    i.add_children(k)
    j.add_children([k, m])
    m.set_parents(j)
    nodes = [a, b, c, d, e, f, g, h, i, j, k, m]

    # The whole enchilada
    order = get_dag_eval_order(["a", "c", "e"], ["g", "k", "m"], nodes)
    assert isinstance(order, list)
    assert len(order) == 12
    assert a in order
    assert b in order
    assert c in order
    assert d in order
    assert e in order
    assert f in order
    assert g in order
    assert h in order
    assert i in order
    assert j in order
    assert k in order
    assert m in order
    assert order.index(a) < order.index(b)
    assert order.index(c) < order.index(d)
    assert order.index(e) < order.index(d)
    assert order.index(b) < order.index(f)
    assert order.index(d) < order.index(f)
    assert order.index(f) < order.index(g)
    assert order.index(d) < order.index(h)
    assert order.index(h) < order.index(i)
    assert order.index(h) < order.index(j)
    assert order.index(i) < order.index(k)
    assert order.index(j) < order.index(k)
    assert order.index(j) < order.index(m)

    # Only g as an output
    order = get_dag_eval_order(["a", "c", "e"], "g", nodes)
    assert isinstance(order, list)
    assert len(order) == 7
    assert a in order
    assert b in order
    assert c in order
    assert d in order
    assert e in order
    assert f in order
    assert g in order
    assert h not in order
    assert i not in order
    assert j not in order
    assert k not in order
    assert m not in order
    assert order.index(a) < order.index(b)
    assert order.index(c) < order.index(d)
    assert order.index(e) < order.index(d)
    assert order.index(b) < order.index(f)
    assert order.index(d) < order.index(f)
    assert order.index(f) < order.index(g)

    # Only k and l as outputs
    order = get_dag_eval_order(["c", "e"], ["k", "m"], nodes)
    assert isinstance(order, list)
    assert len(order) == 8
    assert a not in order
    assert b not in order
    assert c in order
    assert d in order
    assert e in order
    assert f not in order
    assert g not in order
    assert h in order
    assert i in order
    assert j in order
    assert k in order
    assert m in order
    assert order.index(c) < order.index(d)
    assert order.index(e) < order.index(d)
    assert order.index(d) < order.index(h)
    assert order.index(h) < order.index(i)
    assert order.index(h) < order.index(j)
    assert order.index(i) < order.index(k)
    assert order.index(j) < order.index(k)
    assert order.index(j) < order.index(m)


def test_dag_eval_order_cycle():

    # a -> b <------ d -> e
    #       \       /
    #        -> c ->
    a = MyNode("a")
    b = MyNode("b")
    c = MyNode("c")
    d = MyNode("d")
    e = MyNode("e")
    b.set_parents([a, d])
    a.add_children(b)
    d.add_children([b, e])
    c.set_parents(b)
    b.add_children(c)
    d.set_parents(c)
    c.add_children(d)
    e.set_parents(d)
    nodes = [a, b, c, d, e]

    # Should raise runtime error if there's a cycle
    with pytest.raises(RuntimeError):
        _ = get_dag_eval_order("a", "e", nodes)


def test_run_dag_order():

    fit_list = []
    run_list = []

    class ListAdder(Node):
        def fit(self, *args):
            fit_list.append(self.name)

        def run(self, *args):
            run_list.append(self.name)

    # a -> b -> f -> g
    #       /
    # c -> d -> h -> i -> k
    #  /         \    /
    # e           -> j -> m
    a = ListAdder("a")
    b = ListAdder("b")
    c = ListAdder("c")
    d = ListAdder("d")
    e = ListAdder("e")
    f = ListAdder("f")
    g = ListAdder("g")
    h = ListAdder("h")
    i = ListAdder("i")
    j = ListAdder("j")
    k = ListAdder("k")
    m = ListAdder("m")
    b.set_parents(a)
    a.add_children(b)
    d.set_parents([c, e])
    c.add_children(d)
    e.add_children(d)
    f.set_parents([b, d])
    d.add_children(f)
    b.add_children(f)
    g.set_parents(f)
    f.add_children(g)
    h.set_parents(d)
    d.add_children(h)
    i.set_parents(h)
    h.add_children([i, j])
    j.set_parents(h)
    k.set_parents([i, j])
    i.add_children(k)
    j.add_children([k, m])
    m.set_parents(j)
    nodes = [a, b, c, d, e, f, g, h, i, j, k, m]

    # The whole enchilada
    _ = run_dag({}, [], "train", nodes)
    for the_list in [fit_list, run_list]:
        assert isinstance(the_list, list)
        assert len(the_list) == 12
        assert "a" in the_list
        assert "b" in the_list
        assert "c" in the_list
        assert "d" in the_list
        assert "e" in the_list
        assert "f" in the_list
        assert "g" in the_list
        assert "h" in the_list
        assert "i" in the_list
        assert "j" in the_list
        assert "k" in the_list
        assert "m" in the_list
        assert the_list.index("a") < the_list.index("b")
        assert the_list.index("c") < the_list.index("d")
        assert the_list.index("e") < the_list.index("d")
        assert the_list.index("b") < the_list.index("f")
        assert the_list.index("d") < the_list.index("f")
        assert the_list.index("f") < the_list.index("g")
        assert the_list.index("d") < the_list.index("h")
        assert the_list.index("h") < the_list.index("i")
        assert the_list.index("h") < the_list.index("j")
        assert the_list.index("i") < the_list.index("k")
        assert the_list.index("j") < the_list.index("k")
        assert the_list.index("j") < the_list.index("m")

    while len(fit_list) > 0:
        fit_list.pop()
    while len(run_list) > 0:
        run_list.pop()

    # Only g as an output
    _ = run_dag({"a": None, "c": None, "e": None}, "g", "train", nodes)
    for the_list in [fit_list, run_list]:
        assert isinstance(the_list, list)
        assert len(the_list) == 7
        assert "a" in the_list
        assert "b" in the_list
        assert "c" in the_list
        assert "d" in the_list
        assert "e" in the_list
        assert "f" in the_list
        assert "g" in the_list
        assert "h" not in the_list
        assert "i" not in the_list
        assert "j" not in the_list
        assert "k" not in the_list
        assert "m" not in the_list
        assert the_list.index("a") < the_list.index("b")
        assert the_list.index("c") < the_list.index("d")
        assert the_list.index("e") < the_list.index("d")
        assert the_list.index("b") < the_list.index("f")
        assert the_list.index("d") < the_list.index("f")
        assert the_list.index("f") < the_list.index("g")

    while len(fit_list) > 0:
        fit_list.pop()
    while len(run_list) > 0:
        run_list.pop()

    # Only k and l as outputs
    _ = run_dag({"c": None, "e": None}, ["k", "m"], "train", nodes)
    for the_list in [fit_list, run_list]:
        assert isinstance(the_list, list)
        assert len(the_list) == 8
        assert "a" not in the_list
        assert "b" not in the_list
        assert "c" in the_list
        assert "d" in the_list
        assert "e" in the_list
        assert "f" not in the_list
        assert "g" not in the_list
        assert "h" in the_list
        assert "i" in the_list
        assert "j" in the_list
        assert "k" in the_list
        assert "m" in the_list
        assert the_list.index("c") < the_list.index("d")
        assert the_list.index("e") < the_list.index("d")
        assert the_list.index("d") < the_list.index("h")
        assert the_list.index("h") < the_list.index("i")
        assert the_list.index("h") < the_list.index("j")
        assert the_list.index("i") < the_list.index("k")
        assert the_list.index("j") < the_list.index("k")
        assert the_list.index("j") < the_list.index("m")


def test_run_dag_primary_branch():

    run_list = []

    class FeatureCreator(Node):
        def init(self, col, val, n):
            self.data = pd.DataFrame()
            self.data[col] = val * np.ones(n)

        def run(self, *args):
            run_list.append(self.name)
            return self.data

    class FeatureJoiner(Node):
        def run(self, d1, d2):
            run_list.append(self.name)
            return pd.concat((d1, d2), axis=1)

    class FeatureAdder(Node):
        def init(self, col, val):
            self.col = col
            self.val = val

        def run(self, X, y):
            run_list.append(self.name)
            X[self.col] = self.val
            return X, y

    class FeatureTransformer(Node):
        def init(self, col, fn):
            self.col = col
            self.fn = fn

        def run(self, X, y):
            run_list.append(self.name)
            X[self.col] = self.fn(X[self.col])
            return X, y

    class PreFeatureTransformer(Node):
        def init(self, col, fn):
            self.col = col
            self.fn = fn

        def run(self, X):
            run_list.append(self.name)
            X[self.col] = self.fn(X[self.col])
            return X

    fc1 = FeatureCreator("fc1", "a", 1.0, 10)
    fc2 = FeatureCreator("fc2", "b", 2.0, 10)
    fc3 = FeatureCreator("fc3", "c", 3.0, 10)
    fj1 = FeatureJoiner("fj1")
    fj2 = FeatureJoiner("fj2")
    input1 = Input("input1")
    ft0 = PreFeatureTransformer("ft0", "a", lambda x: x + 50)
    p = Primary("primary", ["a", "b"], "c")
    fa1 = FeatureAdder("fa1", "d", 4.0)
    ft1 = FeatureTransformer("ft1", "a", lambda x: x + 100)
    ft2 = FeatureTransformer("ft2", "b", lambda x: x + 200)
    ft3 = FeatureTransformer("ft3", "d", lambda x: x + 10)
    ft4 = FeatureTransformer("ft4", "d", lambda x: x + 20)
    nodes = [fc1, fc2, fc3, fj1, fj2, input1, ft0, p, fa1, ft1, ft2, ft3, ft4]

    # fc1
    #     \
    # fc2 -> fj1
    #            \
    #        fc3 -> fj2                                   -> ft4
    #                   \                                /
    #     input1 -> ft0 -> primary -> fa1 -> ft1 -> ft2 ---> ft3

    fj1.set_parents([fc1, fc2])
    fj2.set_parents([fj1, fc3])
    ft0.set_parents(input1)
    p.set_parents(train_parent=fj2, test_parent=ft0)
    fa1.set_parents(p)
    ft1.set_parents(fa1)
    ft2.set_parents(ft1)
    ft3.set_parents(ft2)
    ft4.set_parents(ft2)

    # Running DAG in training mode should run the training branch pre-primary
    dag_outputs = run_dag({}, ["ft3", "ft4"], "train", nodes)
    assert "ft0" not in run_list
    assert run_list.index("fc1") < run_list.index("fj1")
    assert run_list.index("fc2") < run_list.index("fj1")
    assert run_list.index("fc3") < run_list.index("fj2")
    assert run_list.index("fj1") < run_list.index("fj2")
    assert run_list.index("fj2") < run_list.index("fa1")
    assert run_list.index("fa1") < run_list.index("ft1")
    assert run_list.index("ft1") < run_list.index("ft2")
    assert run_list.index("ft2") < run_list.index("ft3")
    assert run_list.index("ft2") < run_list.index("ft4")
    assert isinstance(dag_outputs, dict)
    assert "ft3" in dag_outputs
    assert "ft4" in dag_outputs
    assert isinstance(dag_outputs["ft3"], tuple)
    assert isinstance(dag_outputs["ft4"], tuple)
    assert isinstance(dag_outputs["ft3"][0], pd.DataFrame)
    assert isinstance(dag_outputs["ft4"][0], pd.DataFrame)
    assert isinstance(dag_outputs["ft3"][1], pd.Series)
    assert isinstance(dag_outputs["ft4"][1], pd.Series)
    assert dag_outputs["ft3"][0].shape[0] == 10
    assert dag_outputs["ft3"][0].shape[1] == 3
    assert dag_outputs["ft4"][0].shape[0] == 10
    assert dag_outputs["ft4"][0].shape[1] == 3
    assert dag_outputs["ft3"][1].shape[0] == 10
    assert dag_outputs["ft4"][1].shape[0] == 10
    assert np.all(dag_outputs["ft3"][0].loc[:, "a"] == 101.0)
    assert np.all(dag_outputs["ft3"][0].loc[:, "b"] == 202.0)
    assert np.all(dag_outputs["ft3"][0].loc[:, "d"] == 14.0)
    assert np.all(dag_outputs["ft4"][0].loc[:, "a"] == 101.0)
    assert np.all(dag_outputs["ft4"][0].loc[:, "b"] == 202.0)
    assert np.all(dag_outputs["ft4"][0].loc[:, "d"] == 24.0)

    # Reset the run list
    while len(run_list) > 0:
        run_list.pop()

    # Running DAG in test mode should only run the test branch pre-primary
    input_data = [{"a": 1, "b": 2}, {"a": 2.0, "b": 3.0}]
    dag_outputs = run_dag(
        {"input1": input_data}, ["ft3", "ft4"], "test", nodes
    )
    assert "ft0" in run_list
    assert "fc1" not in run_list
    assert "fc2" not in run_list
    assert "fc3" not in run_list
    assert "fj1" not in run_list
    assert "fj2" not in run_list
    assert run_list.index("ft0") < run_list.index("fa1")
    assert run_list.index("fa1") < run_list.index("ft1")
    assert run_list.index("ft1") < run_list.index("ft2")
    assert run_list.index("ft2") < run_list.index("ft3")
    assert run_list.index("ft2") < run_list.index("ft4")
    assert isinstance(dag_outputs, dict)
    assert "ft3" in dag_outputs
    assert "ft4" in dag_outputs
    assert isinstance(dag_outputs["ft3"], tuple)
    assert isinstance(dag_outputs["ft4"], tuple)
    assert isinstance(dag_outputs["ft3"][0], pd.DataFrame)
    assert isinstance(dag_outputs["ft4"][0], pd.DataFrame)
    assert dag_outputs["ft3"][1] is None
    assert dag_outputs["ft4"][1] is None
    assert dag_outputs["ft3"][0].shape[0] == 2
    assert dag_outputs["ft3"][0].shape[1] == 3
    assert dag_outputs["ft4"][0].shape[0] == 2
    assert dag_outputs["ft4"][0].shape[1] == 3
    assert dag_outputs["ft3"][0].loc[0, "a"] == 151.0
    assert dag_outputs["ft3"][0].loc[1, "a"] == 152.0
    assert dag_outputs["ft3"][0].loc[0, "b"] == 202.0
    assert dag_outputs["ft3"][0].loc[1, "b"] == 203.0
    assert dag_outputs["ft3"][0].loc[0, "d"] == 14.0
    assert dag_outputs["ft3"][0].loc[1, "d"] == 14.0
    assert dag_outputs["ft4"][0].loc[0, "a"] == 151.0
    assert dag_outputs["ft4"][0].loc[1, "a"] == 152.0
    assert dag_outputs["ft4"][0].loc[0, "b"] == 202.0
    assert dag_outputs["ft4"][0].loc[1, "b"] == 203.0
    assert dag_outputs["ft4"][0].loc[0, "d"] == 24.0
    assert dag_outputs["ft4"][0].loc[1, "d"] == 24.0

    # Reset the run list
    while len(run_list) > 0:
        run_list.pop()

    # Running DAG with a single output
    input_data = [{"a": 1, "b": 2}, {"a": 2.0, "b": 3.0}]
    dag_outputs = run_dag({"input1": input_data}, "ft3", "test", nodes)
    assert "ft0" in run_list
    assert "fc1" not in run_list
    assert "fc2" not in run_list
    assert "fc3" not in run_list
    assert "fj1" not in run_list
    assert "fj2" not in run_list
    assert "ft4" not in run_list  # should NOT have run this one!
    assert run_list.index("ft0") < run_list.index("fa1")
    assert run_list.index("fa1") < run_list.index("ft1")
    assert run_list.index("ft1") < run_list.index("ft2")
    assert run_list.index("ft2") < run_list.index("ft3")
    assert isinstance(dag_outputs, tuple)
    assert isinstance(dag_outputs[0], pd.DataFrame)
    assert dag_outputs[1] is None
    assert dag_outputs[0].shape[0] == 2
    assert dag_outputs[0].shape[1] == 3
    assert dag_outputs[0].loc[0, "a"] == 151.0
    assert dag_outputs[0].loc[1, "a"] == 152.0
    assert dag_outputs[0].loc[0, "b"] == 202.0
    assert dag_outputs[0].loc[1, "b"] == 203.0
    assert dag_outputs[0].loc[0, "d"] == 14.0
    assert dag_outputs[0].loc[1, "d"] == 14.0
