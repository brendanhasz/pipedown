from pipedown.dag import DAG
from pipedown.nodes.base.node import Node
from pipedown.visualization.dag_viewer import get_dag_viewer_html


class MyNode(Node):
    """This is the docstring for my node!

    More info here.

    And even more info here.
    """

    def run(self, X):
        return X


class MyDAG(DAG):
    """This is the docstring for my DAG!

    Lots of high-level info.

    And even more info here.
    """

    def nodes(self):
        return {
            "a": MyNode(),
            "b": MyNode(),
            "c": MyNode(),
            "d": MyNode(),
            "e": MyNode(),
            "f": MyNode(),
        }

    def edges(self):
        return {
            "c": ["a", "b"],
            "d": "c",
            "e": "d",
            "f": "d",
        }


def test_dag_viewer():

    # Define some nodes
    dag = MyDAG()

    # Get the html
    html = get_dag_viewer_html(dag)

    assert isinstance(html, str)

    # Save the html
    with open("test_dag_html.html", "w") as fid:
        fid.write(html)


if __name__ == "__main__":
    test_dag_viewer()
