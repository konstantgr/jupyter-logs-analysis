from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class Cell:
    cell_index: str
    cell_num: int
    cell_source: str = None


class SnapshotBase(ABC):
    @abstractmethod
    def execute_cell(self, cell: Cell) -> None:
        pass

    @abstractmethod
    def create_cell(self, cell: Cell) -> None:
        pass

    @abstractmethod
    def delete_cell(self, cell: Cell) -> None:
        pass

    @abstractmethod
    def update_indices(self, log: pd.Series) -> None:
        pass
