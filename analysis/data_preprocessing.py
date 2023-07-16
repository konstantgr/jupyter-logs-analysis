import ast

import numpy as np
import pandas as pd
import networkx as nx

import code_diff as cd

from pathlib import Path
from collections import defaultdict
from typing import List, Dict

from tqdm import tqdm
from radon.raw import analyze
from radon.visitors import ComplexityVisitor
from ast import literal_eval

from analysis.data_loading import read_hackathon_data
from sequence_processor.sequence import SequenceProcessor
from sequence_processor.snapshots import ExecutiveSnapshot

from streamlit_app.graph_tools import evolution_to_networkx


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

    def calculate_cell_metrics(self, df: pd.DataFrame, store: bool = True) -> pd.DataFrame:
        calculated_metrics = [
            {metric: fun(row.cell_source) for metric, fun
             in self.cell_metrics_mapping[row.event].items()}
            for _, row in df.iterrows()
        ]
        metrics_df = pd.DataFrame(calculated_metrics)
        resulted_df = pd.concat([
            df.reset_index(drop=True), metrics_df.reset_index(drop=True)
        ], axis=1)

        if store:
            self.metrics_dataframes['cell_metrics'] = resulted_df
        return resulted_df

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
        grouped = df.groupby('kernel_id')
        calculated_metrics = []
        for kernel_id, g in tqdm(grouped):
            processor = SequenceProcessor(
                g.drop('expert', axis=1) \
                    .fillna(np.NaN).replace(np.NaN, None).iloc[:]
            )
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

    @staticmethod
    def match_executions(cell_df):

        looking_for_finish = False
        found = {
            'executions': [],
            'unexecuted': [],
            'hagning_finish': []
        }

        cell_df['result'] = cell_df.cell_output.apply(
            lambda x: '' if (x is None) | (x == '[]') else '' + literal_eval(x)[0]['output_type'])

        for i, row in cell_df.iterrows():
            if row.event == 'execute':
                looking_for_finish = i
            if row.event == 'finished_execute':
                if looking_for_finish:
                    found['executions'].append(tuple([looking_for_finish, i]))
                    looking_for_finish = None
                else:
                    found['hagning_finish'].append(i)

        cell_df['execution_time'] = None
        cell_df['execution_result'] = 'ok'
        for execution in found['executions']:
            cell_df.loc[execution[0], 'execution_time'] = cell_df.loc[execution[1], 'time']
            cell_df.loc[execution[0], 'execution_result'] = cell_df.loc[execution[1], 'result']

        return cell_df

    @staticmethod
    def match_edits(cell_df):

        edit_state = None
        found = {
            'edited': [],
            'unedited': [],
            'uncreated': [],
        }

        for i, row in cell_df.iterrows():
            if (row.event == 'finished_execute') or (row.event == 'create'):
                edit_state = i
            if row.event == 'execute':
                if edit_state:
                    found['edited'].append(tuple([edit_state, i]))
                    edit_state = None
                else:
                    found['uncreated'].append(i)

        cell_df['edited_time'] = None
        for edited in found['edited']:
            cell_df.loc[edited[0], 'edited_time'] = cell_df.loc[edited[1], 'time']

        return cell_df

    @staticmethod
    def calculate_graph_metrics(df: pd.DataFrame) -> pd.DataFrame:
        metrics_list = []
        grouped = df.groupby('kernel_id')
        for kernel_id, g in tqdm(list(grouped)[1:]):
            processor = SequenceProcessor(
                g.drop('expert', axis=1).fillna(np.NaN).replace(np.NaN, None).iloc[:]
            )
            snap_num = len(processor.snapshots) - 1
            G = evolution_to_networkx(processor, snap_num + 1)
            H = nx.Graph(G)
            H = nx.convert_node_labels_to_integers(H)

            modularity, average_degree, average_clustering = None, None, None
            if G.nodes:
                modularity = nx.community.modularity(H, nx.community.label_propagation_communities(H))
                average_degree = 2 * len(G.edges) / len(G.nodes)
                average_clustering = nx.average_clustering(H)

            metrics_list.append({
                'kernel_id': kernel_id,
                'modularity': modularity,
                'average_degree': average_degree,
                'average_clustering': average_clustering
            })

        return pd.DataFrame(metrics_list)


if __name__ == '__main__':
    path = Path("data_config.yaml")
    df_hack = read_hackathon_data(path)
    processor = MetricsProcessor()
    print(processor.calculate_graph_metrics(df_hack.iloc[:1000]))

    # print(processor.calculate_notebook_metrics(df_hack.iloc[:1000]).objects_sum.max())
