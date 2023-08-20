import networkx as nx
import pandas as pd

from metrics_base import Metrics


class GraphMetrics(Metrics):

    def __init__(self, G, kernel_id):
        self.G = nx.Graph(G)
        self.graph_metrics_mapping = {
            'modularity': self.get_graph_modularity,
            'average_degree': self.get_graph_average_degree,
            'average_clustering': self.get_graph_average_clustering
        }
        self.kernel_id = kernel_id

    def calculate_metrics(self, _=None):
        calculated_metrics = []

        if not len(self.G.nodes):
            return pd.DataFrame(None, columns=list(self.graph_metrics_mapping.keys()))

        # we can realise it w/o tmp variables
        metrics_tmp = {}
        for metric, fun in self.graph_metrics_mapping.items():
            metrics_tmp[metric] = fun()

        calculated_metrics.append({
            'kernel_id': self.kernel_id,
            **{metric: fun() for metric, fun in self.graph_metrics_mapping.items()}
        })

        return pd.DataFrame(calculated_metrics)

    def get_graph_modularity(self) -> float | None:
        community = nx.community.label_propagation_communities(self.G)
        modularity = nx.community.modularity(self.G, community)
        return modularity

    def get_graph_average_degree(self) -> float | None:
        return 2 * len(self.G.edges) / len(self.G.nodes)

    def get_graph_average_clustering(self) -> float | None:
        return nx.average_clustering(self.G)
