class TableDep:

    graph_dict = {}

    def addTable(self, node, neighbour):
        if node not in self.graph_dict:
            self.graph_dict[node] = [neighbour]
        else:
            self.graph_dict[node].append(neighbour)

    def find_path(self, start, end, path=None):
        path = []
        path = path + [start]
        if start == end:
            return path
        for node in self.graph_dict[start]:
            if node not in path:
                newPath = self.find_path(node, end, path)
                if newPath:
                    return newPath
                return None

    def show_edges(self) -> tuple:
        for node in self.graph_dict:
            for neighbour in self.graph_dict[node]:
                print("(", node, ", ", neighbour, ")")
                return node, neighbour
