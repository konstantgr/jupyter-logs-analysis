import json
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

from notebook_reproduction.notebook_runner.selenium_notebook import SeleniumNotebook


class NotebookRunner:
    def __init__(self, dataset: pd.DataFrame, notebook: SeleniumNotebook):
        self.dataset = dataset
        self.notebook = notebook

    def process_action(
        self,
        row: pd.Series,
        reproduce_topology: bool = True,
    ) -> Optional[int]:
        notebook = self.notebook
        cell_num = 0 if not reproduce_topology else row.cell_num

        match row.event:
            case "execute":
                notebook.change_cell(cell_num=int(cell_num), new_cell_content=row.cell_source)
                error, _ = notebook.execute_cell(cell_num=int(cell_num), with_create=True)
                error_num = int(row.cell_num) if error else None
                return error_num
            case "create":
                notebook.add_cell(cell_num=int(cell_num))
            case "kernel_restarting":
                notebook.restart_kernel()
            case "save_notebook":
                initial_source: List[Dict[str, Any]] = json.loads(row.cell_source)
                for i, cell in enumerate(initial_source):
                    notebook.change_cell(cell_num=i, new_cell_content=cell.get("source", ""))
                    notebook.execute_cell(cell_num=i, with_create=True)
            case _:
                pass

        return None

    def reproduce(self, min_iterations: Optional[int] = None) -> pd.DataFrame:
        df = self.dataset
        total_steps = min_iterations if min_iterations is not None else df.shape[0]
        pbar = tqdm(enumerate(df.iterrows()), total=total_steps)

        try:
            for idx, (row_index, row) in pbar:
                _ = self.process_action(row)
        except Exception as e:
            print(e)
            pass
        finally:
            return df
