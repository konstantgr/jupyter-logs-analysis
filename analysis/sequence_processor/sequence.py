from copy import deepcopy
from typing import List

import numpy as np
import pandas as pd

from analysis.sequence_processor.snapshots import ExecutiveSnapshot


class SequenceProcessor:
    def __init__(self, logs: pd.DataFrame):
        self.logs = logs
        self.snapshots = self.process_sequence()

    def process_sequence(self) -> List[ExecutiveSnapshot]:
        snap_tmp, snap = ExecutiveSnapshot(), ExecutiveSnapshot()
        steps = [snap_tmp]

        for _, log_row in self.logs.iterrows():
            snap_tmp = deepcopy(snap)
            snap_tmp.update_indices(log_row)
            steps.append(snap_tmp)
            snap = snap_tmp

        return steps


def sequence_test() -> None:
    test_data = {
        'event': [
            'execute', 'execute', 'execute', 'create',
            'execute', 'execute', 'execute', 'execute',
            'delete', 'execute', 'execute', 'execute', 'execute', 'execute'],
        'cell_index': [
            'c1', 'c3', 'c5', 'c4',
            'c4', 'c5', 'c2', 'c1',
            'c2', 'c3', 'c4', 'c5', 'c5', 'c5'],
        'cell_num': [
            0, 2, 3, 2,
            3, 4, 1, 0,
            None, 1, 2, 3, 3, 3
        ],
        'cell_source': [None] * 14
    }
    test_df = pd.DataFrame(test_data).replace(np.nan, None)

    processor = SequenceProcessor(test_df)
    for snap in processor.snapshots:
        print(snap.log, snap.index_order)


if __name__ == '__main__':
    sequence_test()
