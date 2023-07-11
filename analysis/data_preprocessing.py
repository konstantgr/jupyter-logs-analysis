import ast

import numpy as np
import pandas as pd

import code_diff as cd

from pathlib import Path
from collections import defaultdict
from typing import List, Dict

from tqdm import tqdm
from radon.raw import analyze
from radon.visitors import ComplexityVisitor

from analysis.data_loading import read_hackathon_data
from sequence_processor.sequence import SequenceProcessor


class MetricsProcessor:
    def __init__(self):
        self.cell_metrics_mapping = defaultdict(dict)
        self.cell_metrics_mapping['execute'] = {
            'objects': self.get_objects_number,
            # 'sloc': self.get_sloc,
            'ccn': self.get_cyclomatic_complexity
        }
        self.metrics_dataframes = defaultdict()

    def _get_ast(self, source: str | None) -> ast.AST:
        try:
            return ast.parse(source)
        except SyntaxError as e:
            code_string = source.splitlines()
            del code_string[e.lineno - 1]
            code_string = '\n'.join(code_string)
            return self._get_ast(code_string)

    @staticmethod
    def _get_code_changes(prev: str | None, cur: str | None) -> List:
        try:
            output = cd.difference(prev, cur, lang="python")
            return output.edit_script()
        except ValueError:
            return []

    def get_objects_number(self, source: str | None) -> int:
        if source is None:
            return 0
        source_ast, variables = self._get_ast(source), set()

        for node in ast.walk(source_ast):
            if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Load, ast.Store)):
                variables.add(node.id)

        return len(variables)

    def get_cyclomatic_complexity(self, source: str | None) -> int:
        if source is None:
            return 0

        source_ast = self._get_ast(source)
        v = ComplexityVisitor.from_ast(source_ast)
        return v.complexity

    @staticmethod
    def get_sloc(source: str | None) -> int:
        if source is None:
            return 0
        try:
            return analyze(source).sloc
        except SyntaxError:
            return len(source.splitlines())

    def get_kernel_transitions(self, kernel_id: str, df: pd.DataFrame) -> List[Dict]:
        transitions = []
        cols = ['cell_index', 'cell_source', 'cell_num']
        executions_list = df[cols].to_numpy()

        prev_idx, prev_source, prev_num = executions_list[0]
        for (idx, source, num) in executions_list[1:]:
            code_changes = self._get_code_changes(prev_source, source) if idx == prev_idx else []
            transitions.append({
                'kernel_id': kernel_id,
                'cell_idx_from': prev_idx, 'cell_num_from': prev_num, 'cell_source_from': prev_source,
                'cell_idx_to': idx, 'cell_num_to': num, 'cell_source_to': source,
                'inner_transition': idx == prev_idx,
                'changes': code_changes
            })
            prev_idx, prev_source, prev_num = idx, source, num

        return transitions

    def get_execution_transitions(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped = df.groupby('event').get_group('execute').groupby('kernel_id')

        transitions = []
        for kernel_id, g in tqdm(grouped):
            transitions += self.get_kernel_transitions(kernel_id, g)

        transitions_df = pd.DataFrame(transitions)

        self.metrics_dataframes['transitions'] = transitions_df
        return transitions_df

    def calculate_cell_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        calculated_metrics = [
            {metric: fun(row.cell_source) for metric, fun
             in self.cell_metrics_mapping[row.event].items()}
            for _, row in tqdm(df.iterrows())
        ]
        metrics_df = pd.DataFrame(calculated_metrics)
        resulted_df = pd.concat([
            df.reset_index(drop=True), metrics_df.reset_index(drop=True)
        ], axis=1)

        self.metrics_dataframes['cell_metrics'] = resulted_df
        return resulted_df

    def calculate_notebook_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped = df.groupby('kernel_id')
        calculated_metrics = []
        for kernel_id, g in tqdm(grouped):
            processor = SequenceProcessor(
                g.drop('expert', axis=1) \
                    .fillna(np.NaN).replace(np.NaN, None).iloc[:]
            )
            for i, snap in enumerate(processor.snapshots[1:]):
                snap.delete_duplicates()
                calculated_metrics.append({
                    'kernel_id': kernel_id,
                    'snap_num': i,
                    'cell_count': len(snap.index_order),
                    **snap.log
                })

        metrics_df = pd.DataFrame(calculated_metrics)
        # resulted_df = pd.concat([
        #     df.reset_index(drop=True), metrics_df.reset_index(drop=True)
        # ], axis=1)

        self.metrics_dataframes['notebook_metrics'] = metrics_df
        return metrics_df


if __name__ == '__main__':
    path = Path("data_config.yaml")
    df_hack = read_hackathon_data(path)
    processor = MetricsProcessor()
    print(processor.calculate_notebook_metrics(df_hack.iloc[:100]))
