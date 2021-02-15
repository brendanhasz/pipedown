from pipedown.nodes.base.node import Node


class Collate(Node):
    def run(self, **args):
        pass
        # TODO: concatenate the input dataframes (vertically)
        # then sort by index
