import ast
import re
from collections import defaultdict

import pandas as pd
from radon.raw import analyze
from radon.visitors import ComplexityVisitor
from tqdm import tqdm

from analysis.metrics.metrics_base import Metrics


class CellsMetrics(Metrics):

    def __init__(self):
        self.cell_metrics_mapping = defaultdict(dict)
        self.cell_metrics_mapping['execute'] = {
            'objects': self.get_objects_number,
            'sloc': self.get_sloc,
            'ccn': self.get_cyclomatic_complexity,
            'comments': self.get_comments
        }
        self.metrics_dataframes = defaultdict()

    def get_all_metrics(self):
        return [
            metric for metric_type in self.cell_metrics_mapping.keys()
            for metric in self.cell_metrics_mapping[metric_type].keys()
        ]

    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        cell_metrics = self.calculate_cell_metrics(df)
        aggregated_metrics = self.aggregate_cells_metrics(cell_metrics)

        return aggregated_metrics

    def calculate_cell_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        calculated_metrics = [
            {metric: fun(row.cell_source) for metric, fun
             in self.cell_metrics_mapping[row.event].items()}
            for _, row in tqdm(df.iterrows())
        ]
        metrics_df = pd.DataFrame(calculated_metrics)
        return pd.concat([
            df.reset_index(drop=True), metrics_df.reset_index(drop=True)
        ], axis=1)
        # dfs = [df]
        # for event, metrics in self.cell_metrics_mapping.items():
        #     mask = (df.event.values == event)
        #     dfs.extend(
        #         [df[mask].cell_source.apply(fun).rename(metric)
        #          for metric, fun in metrics.items()]
        #     )
        #
        # df_merged = reduce(lambda left, right: left.join(right), dfs)
        # return df_merged

    def aggregate_cells_metrics(self, df_metrics) -> pd.DataFrame:

        agg_list = ['kernel_id'] + self.get_all_metrics()
        df_metrics = df_metrics.loc[:, agg_list].groupby('kernel_id').agg(['mean', 'sum'])

        return df_metrics

    def _get_ast(self, source: str | None) -> ast.AST:
        try:
            return ast.parse(source)
        except SyntaxError as e:
            code_string = source.splitlines()
            del code_string[e.lineno - 1]
            code_string = '\n'.join(code_string)
            return self._get_ast(code_string)

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

    @staticmethod
    def get_comments(source: str | None, loc: bool = True) -> int:
        def get_comments_len(input_string: str) -> int:
            pattern = r'#(.*)'
            matches = re.findall(pattern, input_string, re.MULTILINE)
            return sum([len(match.strip()) for match in matches])

        if source is None:
            return 0
        try:
            return analyze(source).comments if loc else get_comments_len(source)
        except SyntaxError:
            return len(source.splitlines())
