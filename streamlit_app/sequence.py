import pandas as pd

from copy import copy, deepcopy
from typing import List

from snapshot import Snapshot


class NotebookActionsSequence:
    def __init__(self, logs: pd.DataFrame):
        self.allowed_actions = []
        self.logs = logs
        self.snapshots = self.process_sequence()

    def process_sequence(self) -> List[Snapshot]:
        snap_tmp = Snapshot()
        snap = Snapshot()
        steps = [snap_tmp]
        for _, log_row in self.logs.iterrows():
            snap_tmp = deepcopy(snap)
            snap_tmp.recalculate_indices(log_row)
            steps.append(snap_tmp)
            snap = snap_tmp

        print('here')
        return steps


