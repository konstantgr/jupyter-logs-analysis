from typing import Optional

import networkx as nx
import pandas as pd
from tqdm import tqdm

from analysis.metrics.metrics_base import Metrics
from analysis.metrics.utils.graph_tools import graphviz2networkx, dataframe_to_graphviz


class GraphMetrics(Metrics):

    def __init__(self):
        self.graph_metrics_mapping = {
            'modularity': self.get_graph_modularity,
            'average_degree': self.get_graph_average_degree,
            'average_clustering': self.get_graph_average_clustering
        }

    def calculate_metrics(self, df: pd.DataFrame, progress: bool = True) -> pd.DataFrame:
        pbar = tqdm(df.groupby('kernel_id')) if progress else df.groupby('kernel_id')
        return pd.concat([
            self.calculate_kernel_metrics(df_kernel, kernel_id)
            for (kernel_id, df_kernel) in pbar
        ])

    def calculate_kernel_metrics(self, df: pd.DataFrame, kernel_id: Optional[str] = None) -> pd.DataFrame:
        if 'kernel_id' in list(df):
            kernel_id = df.kernel_id.iloc[0]

        gv = dataframe_to_graphviz(df)
        G = graphviz2networkx(gv)

        if not len(G.nodes):
            return pd.DataFrame(None, columns=list(self.graph_metrics_mapping.keys()))

        calculated_metrics = [{
            'kernel_id': kernel_id,
            **{metric: fun(G) for metric, fun in self.graph_metrics_mapping.items()}
        }]

        return pd.DataFrame(calculated_metrics)

    @staticmethod
    def get_graph_modularity(G: nx.Graph) -> float | None:
        if not len(G.nodes):
            return None
        H = nx.Graph(G)
        community = nx.community.label_propagation_communities(H)
        modularity = nx.community.modularity(H, community)
        return modularity

    @staticmethod
    def get_graph_average_degree(G: nx.Graph) -> float | None:
        if not len(G.nodes):
            return None
        return 2 * len(G.edges) / len(G.nodes)

    @staticmethod
    def get_graph_average_clustering(G: nx.Graph) -> float | None:
        if not len(G.nodes):
            return None
        H = nx.Graph(G)
        return nx.average_clustering(H)
