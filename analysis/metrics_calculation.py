import ast
import pandas as pd

from typing import Dict
from radon.complexity import ComplexityVisitor
from radon.raw import analyze
from sequence_processor.snapshots import ExecutiveSnapshot


class MetricsCalculator:
    def __init__(self, source: str = ""):
        self.metrics_mapping = {
            'objects': self.get_objects_number,
            'sloc': self.get_sloc,
            'ccn': self.get_cyclomatic_complexity
        }
        self.source = source
        self.ast = self._get_ast(source)

    def _get_ast(self, source: str) -> ast.AST:
        try:
            return ast.parse(source)
        except SyntaxError as e:
            code_string = source.splitlines()
            del code_string[e.lineno - 1]
            code_string = '\n'.join(code_string)
            return self._get_ast(code_string)

    def get_objects_number(self):
        variables = set()

        for node in ast.walk(self.ast):
            if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Load, ast.Store)):
                variables.add(node.id)

        return len(variables)

    def get_cyclomatic_complexity(self):
        v = ComplexityVisitor.from_ast(self.ast)
        return v.complexity

    def get_sloc(self):
        try:
            return analyze(self.source).sloc
        except Exception:
            return None

    def calculate_metrics(self):
        return {
            metric_name: metric_fun()
            for metric_name, metric_fun in self.metrics_mapping.items()
        }


def get_snapshot_complexity(snap: ExecutiveSnapshot) -> Dict:
    notebook_metrics = []
    calculator = MetricsCalculator()

    for (cell_index, cell_num) in snap.index_order:
        source = snap.index_source_mapping[cell_index]
        if source is None:
            continue

        calculator = MetricsCalculator(source)
        cell_metrics = calculator.calculate_metrics()
        notebook_metrics.append(cell_metrics)

    metrics_df = pd.DataFrame(notebook_metrics)
    if not list(metrics_df):
        metrics_sum = {
            f'{metric_name}_sum': 0
            for metric_name in calculator.metrics_mapping.keys()
        }
        metrics_mean = {
            f'{metric_name}_mean': 0
            for metric_name in calculator.metrics_mapping.keys()
        }
        return {**metrics_sum, **metrics_mean}

    metrics_sum = {
        f'{metric_name}_sum': metrics_df[metric_name].sum()
        for metric_name in calculator.metrics_mapping.keys()
    }
    metrics_mean = {
        f'{metric_name}_mean': metrics_df[metric_name].mean()
        for metric_name in calculator.metrics_mapping.keys()
    }
    return {**metrics_sum, **metrics_mean}
