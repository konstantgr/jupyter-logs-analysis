from collections import defaultdict
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from analysis.data_loading import read_hackathon_data
from analysis.sequence_processor.sequence import SequenceProcessor
from analysis.sequence_processor.snapshots import ExecutiveSnapshot


class MetricsProcessor:
    def __init__(self):
        self.cell_metrics_mapping = defaultdict(dict)
        self.cell_metrics_mapping['execute'] = {
            'objects': self.get_objects_number,
            'sloc': self.get_sloc,
            'ccn': self.get_cyclomatic_complexity
        }
        self.metrics_dataframes = defaultdict()

    def aggregate_cells_metrics(self, snap: ExecutiveSnapshot, delete_duplicates: bool = True) -> dict[str, float]:
        if delete_duplicates:
            snap.delete_duplicates()

        sources = [
            snap.index_source_mapping[idx]
            for idx, _ in snap.index_order
        ]
        df_tmp = pd.DataFrame({'cell_source': sources})
        df_tmp['event'] = 'execute'

        metrics = self.cell_metrics_mapping['execute'].keys()
        df_tmp = self.calculate_cell_metrics(df_tmp, store=False)
        df_metrics = df_tmp[metrics]

        metrics_dict = df_metrics.agg(['mean', 'sum'])
        metrics_dict = {f'{k1}_{k2}': v2
                        for k1, v1 in metrics_dict.items()
                        for k2, v2 in v1.items()}
        return metrics_dict

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


if __name__ == '__main__':
    path = Path("data_config.yaml")
    df_hack = read_hackathon_data(path)
    metrics_processor = MetricsProcessor()
    graph_metrics = metrics_processor.calculate_graph_metrics(df_hack.iloc[:1000])
    print(graph_metrics.head())
