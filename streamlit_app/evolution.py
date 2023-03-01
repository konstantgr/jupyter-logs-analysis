from dataclasses import dataclass
from collections import deque
from copy import deepcopy
from typing import List

import pandas as pd


@dataclass
class Cell:
    cell_index: str
    cell_num: int
    cell_source: str


class Snapshot:
    def __init__(self):
        self.actions_mapping = {
            'execute': self.execute_cell,
            'rendered': self.execute_cell,
            'create': self.create_cell,
            'delete': self.delete_cell,
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
        num = [i for i, c in enumerate(self.cells_list) if c.cell_index == cell.cell_index][0]
        del self.cells_list[num]

        for i in range(num, len(self.cells_list)):
            self.cells_list[i].cell_num -= 1

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
