import ast

import pandas as pd
from pathlib import Path

from radon.raw import analyze
from radon.visitors import ComplexityVisitor
from tqdm import tqdm
from analysis.data_loading import read_hackathon_data
from collections import defaultdict


class MetricsProcessor:
    def __init__(self):
        self.metrics_mapping = defaultdict(dict)
        self.metrics_mapping['execute'] = {
            'objects': self.get_objects_number,
            # 'sloc': self.get_sloc,
            'ccn': self.get_cyclomatic_complexity
        }
        self.metrics_dataframes = defaultdict()

    def _get_ast(self, source: str) -> ast.AST:
        try:
            return ast.parse(source)
        except SyntaxError as e:
            code_string = source.splitlines()
            del code_string[e.lineno - 1]
            code_string = '\n'.join(code_string)
            return self._get_ast(code_string)

    def get_objects_number(self, source: str | None):
        if source is None:
            return 0
        source_ast, variables = self._get_ast(source), set()

        for node in ast.walk(source_ast):
            if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Load, ast.Store)):
                variables.add(node.id)

        return len(variables)

    def get_cyclomatic_complexity(self, source: str | None):
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

    def calculate_cell_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        calculated_metrics = [
            {metric: fun(row.cell_source) for metric, fun
             in self.metrics_mapping[row.event].items()}
            for _, row in tqdm(df.iterrows())
        ]
        metrics_df = pd.DataFrame(calculated_metrics).reset_index(drop=True)
        resulted_df = pd.concat([df.reset_index(drop=True), metrics_df], axis=1)

        self.metrics_dataframes['cell_metrics'] = resulted_df
        return resulted_df


if __name__ == '__main__':
    path = Path("data_config.yaml")
    df_hack = read_hackathon_data(path)
    processor = MetricsProcessor()
    print(processor.calculate_cell_metrics(df_hack))
