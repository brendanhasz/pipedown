import os
import re

from graphviz import Digraph
from jinja2 import Template


def get_dag_viewer_html(dag):

    # Get the graphviz svg for the dag
    dag_svg = get_dag_svg(dag)

    # Get the node descriptions / data
    info_pane_html = get_info_pane_html(dag)

    # Load static files
    html_css = get_static_file("html.css")
    # TODO: pan / zoom javascript
    main_html = get_static_file(
        "main.html",
        dag_name=type(dag).__name__,
        html_css=html_css,
        dag_svg=dag_svg,
        info_pane_html=info_pane_html,
    )

    # Return the html
    return main_html


def get_info_pane_html(dag):

    # Template for info panel
    info_template = Template(
        """
    <div id="{{ name }}-info">
        <h1>{{ name }}</h1>
        <p>{{ description }}</p>
    </div>
    """
    )

    # First get the DAG description
    html = info_template.render(
        name=type(dag).__name__, description=type(dag).__doc__
    )

    # Get the descriptions for each node
    for name, node in dag.get_node_dict().items():
        html += info_template.render(name=name, description=node.__doc__)

    return html


def get_dag_svg(dag):

    # Get graphviz-generated svg
    svg = get_graphviz_svg(dag)

    # Get just the svg
    # TODO: re.search("<svg(.*)</svg>", svg) wasn't working for some reason?
    svg = svg[svg.find("<svg") : svg.find("</svg>") + 6]

    # Remove graphviz-generated style
    svg = remove_graphviz_style(svg)

    # Get the templated css
    svg_css = get_static_file("svg.css")

    # Insert the style into the svg
    svg = re.sub(">.*?<", f">\n<style>\n{svg_css}\n</style>\n<", svg, 1)

    return svg


def get_graphviz_svg(dag):

    # Create the graph
    graph = Digraph(format="svg")

    # Create the nodes
    for node in dag.get_node_dict():
        graph.node(node, shape="box", style="rounded", id=node)
        # TODO: format the node based on type of node

    # Create each node and its edges
    edge_kwargs = {"arrowsize": "0.7"}
    for child, parents in dag.edges().items():

        # Create the edges
        if isinstance(parents, str):
            graph.edge(parents, child, **edge_kwargs)
        elif isinstance(parents, list):
            for p in parents:
                graph.edge(p, child, **edge_kwargs)
        elif isinstance(parents, dict):
            graph.edge(parents["test"], child, **edge_kwargs)
            graph.edge(parents["train"], child, **edge_kwargs)

    return graph.pipe().decode("utf-8")


def remove_graphviz_style(svg: str):

    # Remove fill and stroke info from all polygons
    svg = svg.replace('fill="none" stroke="black" ', "")
    svg = svg.replace('fill="black" stroke="black" ', "")

    # Remove white background polygon
    svg = re.sub('<polygon fill="white".*?/>', "", svg)

    return svg


def get_static_file(filename, **kwargs):
    """Load a static file, optionally jinja templating it with key/values"""
    abs_filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "static", filename
    )
    with open(abs_filename, "r") as fid:
        return Template(fid.read()).render(**kwargs)
