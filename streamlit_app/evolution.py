from dataclasses import dataclass
from collections import deque
from copy import deepcopy
from typing import List
from enum import Enum

import pandas as pd


@dataclass
class Cell:
    cell_index: str
    cell_num: int
    cell_source: str


class ActionName(Enum):
    EXECUTE = "execute"
    RENDERED = "rendered"
    CREATE = "create"
    DELETE = "delete"


class Snapshot:
    def __init__(self):
        self.actions_mapping = {
            ActionName.EXECUTE.value: self.execute_cell,
            ActionName.RENDERED.value: self.execute_cell,
            ActionName.CREATE.value: self.create_cell,
            ActionName.DELETE.value: self.delete_cell,
        }
        self.cells_list = deque()
        self.log = None

    def recalculate_indices(self, log: pd.Series) -> None:
        action, cell_index, cell_num, cell_source = log.event, log.cell_index, log.cell_num, log.cell_source
        cell_num = int(cell_num) if cell_num is not None else cell_num
        self.log = {
            'event': action,
            'cell_num': cell_num,
            'cell_index': cell_index
        }

        cell = Cell(cell_index=cell_index, cell_num=cell_num, cell_source=cell_source)
        if action in self.actions_mapping:
            self.actions_mapping[action](cell)

    def delete_cell(self, cell: Cell) -> None:
        try:
            num, = [
                i for i, c in enumerate(self.cells_list)
                if c.cell_index == cell.cell_index
            ]
            del self.cells_list[num]

            for i in range(num, len(self.cells_list)):
                self.cells_list[i].cell_num -= 1

        except ValueError:
            print(f"Value error with cell {cell}")

    def create_cell(self, cell: Cell) -> None:

        new_cell_index = cell.cell_num + 1
        new_cell = cell
        new_cell.cell_num = new_cell_index

        if new_cell_index < len(self.cells_list) - 1:
            self.cells_list.insert(new_cell_index, new_cell)
        else:
            self.cells_list.append(new_cell)

        last_cell_index = len(self.cells_list) - 1
        if new_cell_index < last_cell_index:
            for i in range(new_cell_index + 1, len(self.cells_list)):
                self.cells_list[i].cell_num += 1

    def execute_cell(self, cell: Cell) -> None:
        if cell.cell_num == 0 and not len(self.cells_list):
            self.cells_list.append(cell)

        elif len(self.cells_list):
            self.cells_list[cell.cell_num].cell_source = cell.cell_source


class SnapshotMetrics:
    def __init__(self, snap: Snapshot):
        self.snapshot = snap

    def process_snapshot_metrics(self):
        # TODO: metrics
        pass


class NotebookEvolution:
    def __init__(self, logs: pd.DataFrame):
        self.logs = logs
        self.snapshots = self.process_evolution()

    def process_evolution(self) -> List[Snapshot]:
        snap_tmp = Snapshot()
        snap = Snapshot()
        snap_tmp.cells_list = snap.cells_list
        steps = [snap_tmp]
        for _, log_row in self.logs.iterrows():
            snap_tmp = Snapshot()
            snap_tmp.cells_list = deepcopy(snap.cells_list)

            snap_tmp.recalculate_indices(log_row)
            steps.append(snap_tmp)
            snap = snap_tmp

        return steps


class EvolutionMetrics:
    def __init__(self, evol: NotebookEvolution):
        self.metrics_mapping = {
            'deleted_cells': self.get_deleted_cells,
            'kernel_restarts': self.dummy_func,
            'kernel_interruption': self.dummy_func,
            'notebook_names': self.dummy_func,
        }
        self.evol = evol

    def process_evolution_metrics(self):
        # TODO: metrics
        metrics = {}
        for metric, metric_function in self.metrics_mapping.items():
            metrics[metric] = metric_function()

    def get_deleted_cells(self):
        cells = [
            snap.log.get('cell_index') for snap
            in filter(
                lambda x: x.log.get("event") == "delete",
                self.evol.snapshots
            )
        ]

        return cells

    def dummy_func(self):
        evol = self.evol
        return None
