from metrics_graph import GraphMetrics
from graph_tools import evolution_to_networkx
from tqdm import tqdm
import pandas as pd
from ..sequence_processor.sequence import SequenceProcessor


class MetricsProcessor():

    def __init__(self, clean_df):
        self.clean_df = clean_df



    def calculate_graph_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        graph_metrics = []

        grouped = self.clean_df.groupby('kernel_id')
        for kernel_id, g in tqdm(list(grouped)[1:]):
            processor = SequenceProcessor(g)
            G = evolution_to_networkx(processor, len(processor.snapshots))
            GM = GraphMetrics(G, kernel_id)
            metrics = GM.calculate_metrics
            graph_metrics.append(metrics)

        return pd.concat(graph_metrics)

    def calculate_notebook_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped = self._preprocess_dataframe_columns(df).groupby('kernel_id')
        calculated_metrics = []
        for kernel_id, g in tqdm(grouped):
            processor = SequenceProcessor(g)
            for i, snap in enumerate(processor.snapshots[1:]):
                snap.delete_duplicates()
                try:
                    metrics_dict = self.aggregate_cells_metrics(snap)
                except KeyError:
                    metrics_dict = {}

                calculated_metrics.append({
                    'kernel_id': kernel_id,
                    'snap_num': i,
                    'cell_count': len(snap.index_order),
                    **metrics_dict,
                    **snap.log
                })

        metrics_df = pd.DataFrame(calculated_metrics)
        self.metrics_dataframes['notebook_metrics'] = metrics_df
        return metrics_df
