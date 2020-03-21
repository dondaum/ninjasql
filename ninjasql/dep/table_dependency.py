import networkx as nx


class TableDep:

    def __init__(self):
        self.graph = nx.DiGraph()

    def addTable(self, name, depends_on):
        self.graph.add_edge(name, depends_on)

    def show_edges(self) -> list:
        """
        method that list all edges of the graph
        """
        return list(self.graph.edges())

    def find_path(self) -> list:
        """
        method that returns path in topological Sorting
        """
        return list(reversed(list(nx.topological_sort(self.graph))))

    def find_batch_path(self):
        """
        method that returns the topological Sorting with a batch
        grouping of all tasks that can run at the same time
        """
        return list(self._create_batch_order())

    def _create_batch_order(self):
        """
        method that create batches of nodes in order to run concurrent or
        parallel task loads
        """
        inverse_graph = nx.DiGraph()
        for name, depends_on in self.show_edges():
            inverse_graph.add_edge(depends_on, name)
        indegree_map = {v: d for v, d in inverse_graph.in_degree() if d > 0}
        zero_indegree = [v for v, d in inverse_graph.in_degree() if d == 0]
        while zero_indegree:
            yield zero_indegree
            new_zero_indegree = []
            for v in zero_indegree:
                for _, child in inverse_graph.edges(v):
                    indegree_map[child] -= 1
                    if not indegree_map[child]:
                        new_zero_indegree.append(child)
            zero_indegree = new_zero_indegree
