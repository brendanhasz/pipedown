from pipedown.nodes.base.node import Node
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


def test_dag_eval_order_convergence():
    pass
    # TODO

def test_dag_eval_order_divergence_and_convergence():
    pass
    # TODO
