import pytest

from pipedown.nodes.base.node import Node
from pipedown.pipeline.dag import get_dag_eval_order


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
    # e           -> j -> l
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
    l = MyNode("l")
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
    j.add_children([k, l])
    l.set_parents(j)
    nodes = [a, b, c, d, e, f, g, h, i, j, k, l]

    # The whole enchilada
    order = get_dag_eval_order(["a", "c", "e"], ["g", "k", "l"], nodes)
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
    assert l in order
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
    assert order.index(j) < order.index(l)

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
    assert l not in order
    assert order.index(a) < order.index(b)
    assert order.index(c) < order.index(d)
    assert order.index(e) < order.index(d)
    assert order.index(b) < order.index(f)
    assert order.index(d) < order.index(f)
    assert order.index(f) < order.index(g)

    # Only k and l as outputs
    order = get_dag_eval_order(["c", "e"], ["k", "l"], nodes)
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
    assert l in order
    assert order.index(c) < order.index(d)
    assert order.index(e) < order.index(d)
    assert order.index(d) < order.index(h)
    assert order.index(h) < order.index(i)
    assert order.index(h) < order.index(j)
    assert order.index(i) < order.index(k)
    assert order.index(j) < order.index(k)
    assert order.index(j) < order.index(l)


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
        order = get_dag_eval_order("a", "e", nodes)
