import pandas as pd

from copy import deepcopy
from typing import List

from snapshot import Snapshot


class NotebookActionsSequence:
    def __init__(self, logs: pd.DataFrame):
        self.logs = logs
        self.snapshots = self.process_sequence()

    def process_sequence(self) -> List[Snapshot]:
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

