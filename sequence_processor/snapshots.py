import pandas as pd
import json

from enum import StrEnum
from collections import deque

from .snapshot_base import SnapshotBase, Cell


class ActionName(StrEnum):
    EXECUTE = "execute"
    RENDERED = "rendered"
    CREATE = "create"
    DELETE = "delete"
    SAVE = "save_notebook"


class ExecutiveSnapshot(SnapshotBase):
    def __init__(self):
        self.actions_mapping = {
            ActionName.EXECUTE: self.execute_cell,
            ActionName.RENDERED: self.execute_cell,
            ActionName.CREATE: self.create_cell,
            ActionName.DELETE: self.delete_cell,
        }
        self.index_order = deque()
        self.index_num_mapping = {}
        self.index_source_mapping = {}
        self.log = None

    def display_notebook(self, cell_separator: str = "[CELL_SEPARATOR]") -> None:
        tmp = [
            i[0] for i in self.index_order if
            i[0] in self.index_source_mapping.keys()
        ]
        for idx in tmp:
            print(f"[CELL INDEX {idx}]\n", self.index_source_mapping[idx])
            print(cell_separator)

    def create_cell(self, cell: Cell) -> None:
        current_index, current_num = cell.cell_index, cell.cell_num
        if current_index in self.index_num_mapping:
            return

        self.index_num_mapping[current_index] = current_num + 1
        for i, (index_i, num_i) in enumerate(self.index_order):
            if num_i >= current_num + 1:
                self.index_num_mapping[index_i] += 1
                self.index_order[i] = (index_i, num_i + 1)

        self.index_order.append((current_index, current_num + 1))
        self.index_order = sorted(self.index_order, key=lambda x: x[1])

    def delete_cell(self, cell: Cell) -> None:
        current_index = cell.cell_index
        current_num = (
            self.index_num_mapping[current_index]
            if current_index in self.index_num_mapping else cell.cell_num
        )
        if current_index in self.index_num_mapping:
            list_index_to_remove = None
            for i, (index_i, num_i) in enumerate(self.index_order):
                if index_i == current_index:
                    list_index_to_remove = i
                    if current_num is None:
                        current_num = num_i
                    break

            del self.index_num_mapping[current_index]
            del self.index_order[list_index_to_remove]

        if current_num is None:
            return

        for i, (index_i, num_i) in enumerate(self.index_order):
            if num_i > current_num:
                self.index_num_mapping[index_i] -= 1
                self.index_order[i] = (index_i, num_i - 1)

    def execute_cell(self, cell: Cell) -> None:
        current_index, current_num = cell.cell_index, cell.cell_num
        if (((current_index, current_num) in self.index_order)
                and (current_index in self.index_num_mapping)):
            return

        list_indices_to_delete = []
        for i, (index_i, num_i) in enumerate(self.index_order):
            if (index_i == current_index) and (num_i != current_num):
                list_indices_to_delete.append(i)

        for i in list_indices_to_delete:
            del self.index_order[i]

        self.index_num_mapping[current_index] = current_num
        self.index_order.append((current_index, current_num))
        self.index_order = sorted(self.index_order, key=lambda x: x[1])

    def initialize_indices(self, cells_json: str) -> None:
        self.index_order = deque()
        for num, cell_dict in enumerate(json.loads(cells_json)):
            cell_index = cell_dict['id']
            self.index_source_mapping[cell_index] = cell_dict['source']
            self.index_num_mapping[cell_index] = num
            self.index_order.append((cell_index, num))

    def delete_duplicates(self) -> None:
        list_index_to_delete = []
        for i, (index_i, num_i) in enumerate(self.index_order):
            for j, (index_j, num_j) in enumerate(self.index_order):
                if j <= i:
                    continue
                if num_i == num_j and index_i != index_j:
                    list_index_to_delete.append(j)
                    if index_j in self.index_num_mapping:
                        del self.index_num_mapping[index_j]

        self.index_order = [
            p for i, p in enumerate(self.index_order)
            if i not in set(list_index_to_delete)
        ]

    def update_indices(self, log: pd.Series) -> None:
        action, cell_index, cell_num, cell_source = log.event, log.cell_index, log.cell_num, log.cell_source
        cell_num = int(cell_num) if cell_num is not None else cell_num
        self.log = {
            'event': action,
            'cell_num': cell_num,
            'cell_index': cell_index
        }
        if action == "save_notebook":
            self.initialize_indices(cell_source)
            return

        self.index_source_mapping[cell_index] = cell_source

        cell = Cell(cell_index=cell_index, cell_num=cell_num)

        if action in self.actions_mapping:
            self.actions_mapping[action](cell)
