from typing import List, Dict

import code_diff as cd
import pandas as pd
from tqdm import tqdm

from analysis.metrics.metrics_base import Metrics


class TransitionMetrics(Metrics):
    def calculate_metrics(self, df):
        df_ex = self.get_execution_transitions(df)
        df_ev = self.get_event_transitions(df)
        return pd.concat([df_ex, df_ev])

    @staticmethod
    def _get_code_changes(prev: str | None, cur: str | None) -> List:
        try:
            output = cd.difference(prev, cur, lang="python")
            return output.edit_script()
        except ValueError:
            return []

    def get_execution_transitions(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped = df.groupby('event').get_group('execute').groupby('kernel_id')

        transitions = []
        for kernel_id, g in tqdm(grouped):
            transitions += self.get_kernel_transitions(kernel_id, g)

        transitions_df = pd.DataFrame(transitions)

        return transitions_df

    def get_kernel_transitions(self, kernel_id: str, df: pd.DataFrame) -> List[Dict]:
        transitions = []
        cols = ['cell_index', 'cell_source', 'cell_num', 'label']
        executions_list = df[cols].to_numpy()

        prev_idx, prev_source, prev_num, prev_label = executions_list[0]
        for (idx, source, num, label) in executions_list[1:]:
            code_changes = self._get_code_changes(prev_source, source) if idx == prev_idx else []
            transitions.append({
                'kernel_id': kernel_id,
                'cell_idx_from': prev_idx, 'cell_num_from': prev_num, 'cell_source_from': prev_source,
                'cell_idx_to': idx, 'cell_num_to': num, 'cell_source_to': source,
                'cell_label_from': prev_label, 'cell_label_to': label,
                'inner_transition': idx == prev_idx,
                'changes': code_changes,
                'type': 'execution_transition'
            })
            prev_idx, prev_source, prev_num, prev_label = idx, source, num, label

        return transitions

    def get_event_transitions(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped = df.groupby('kernel_id')

        transitions = []
        for kernel_id, g in tqdm(grouped):
            events_list = g.event.to_numpy()
            events_transitions = list(zip(events_list, events_list[1:]))
            transitions += [
                {"kernel_id": kernel_id, 'event_from': start, 'event_to': finish, 'type': 'event_transition'}
                for (start, finish) in events_transitions
            ]

        transitions_df = pd.DataFrame(transitions)

        return transitions_df
