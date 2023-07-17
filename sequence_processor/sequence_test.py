import random

import numpy as np
import pandas as pd

from sequence_processor.sequence import SequenceProcessor

random.seed(42)


def test_create_cell():
    input_data = {
        'event': ["create"],
        'cell_index': ["c1"],
        'cell_num': [0],
        'cell_source': [None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 2
    assert snap.index_source_mapping == {"c1": None}
    assert snap.index_num_mapping == {"c1": 1}
    assert snap.index_order == [("c1", 1)]


def test_execute_known_cell():
    input_data = {
        'event': ["create", 'execute'],
        'cell_index': ["c1", 'c1'],
        'cell_num': [0, 1],
        'cell_source': [None, "source_c1"]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 3
    assert snap.index_source_mapping == {"c1": "source_c1"}
    assert snap.index_num_mapping == {"c1": 1}
    assert snap.index_order == [("c1", 1)]


def test_execute_unknown_cell():
    input_data = {
        'event': ["create", 'execute'],
        'cell_index': ["c1", 'c2'],
        'cell_num': [0, 5],
        'cell_source': [None, "source_c2"]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 3
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2"}
    assert snap.index_num_mapping == {"c1": 1, "c2": 5}
    assert snap.index_order == [("c1", 1), ("c2", 5)]


def test_create_cell_2():
    input_data = {
        'event': ["create", 'execute', 'create'],
        'cell_index': ["c1", 'c2', 'c3'],
        'cell_num': [0, 5, 2],
        'cell_source': [None, "source_c2", None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 4
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2", "c3": None}
    assert snap.index_num_mapping == {"c1": 1, "c2": 6, "c3": 3}
    assert snap.index_order == [("c1", 1), ("c3", 3), ("c2", 6)]


def test_create_cell_3():
    input_data = {
        'event': ["create", 'execute', 'create', 'create'],
        'cell_index': ["c1", 'c2', 'c3', 'c4'],
        'cell_num': [0, 5, 2, 6],
        'cell_source': [None, "source_c2", None, None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 5
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2", "c3": None, "c4": None}
    assert snap.index_num_mapping == {"c1": 1, "c2": 6, "c3": 3, "c4": 7}
    assert snap.index_order == [("c1", 1), ("c3", 3), ("c2", 6), ("c4", 7)]


def test_delete_cell():
    input_data = {
        'event': ["create", 'execute', 'create', 'create', 'delete'],
        'cell_index': ["c1", 'c2', 'c3', 'c4', 'c3'],
        'cell_num': [0, 5, 2, 6, 3],
        'cell_source': [None, "source_c2", None, None, None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 6
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2", "c3": None, "c4": None}
    assert snap.index_num_mapping == {"c1": 1, "c2": 5, "c4": 6}
    assert snap.index_order == [("c1", 1), ("c2", 5), ("c4", 6)]


def test_delete_cell_2():
    input_data = {
        'event': ["create", 'execute', 'create', 'create', 'delete'],
        'cell_index': ["c1", 'c2', 'c3', 'c4', 'c5'],
        'cell_num': [0, 5, 2, 6, 4],
        'cell_source': [None, "source_c2", None, None, None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 6
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2", "c3": None, "c4": None, "c5": None}
    assert snap.index_num_mapping == {"c1": 1, "c2": 5, "c3": 3, "c4": 6}
    assert snap.index_order == [("c1", 1), ("c3", 3), ("c2", 5), ("c4", 6)]


def test_delete_cell_3():
    input_data = {
        'event': ["create", 'execute', 'create', 'create', 'delete'],
        'cell_index': ["c1", 'c2', 'c3', 'c4', 'c5'],
        'cell_num': [0, 5, 2, 6, 10],
        'cell_source': [None, "source_c2", None, None, None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 6
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2", "c3": None, "c4": None, "c5": None}
    assert snap.index_num_mapping == {"c1": 1, "c2": 6, "c3": 3, "c4": 7}
    assert snap.index_order == [("c1", 1), ("c3", 3), ("c2", 6), ("c4", 7)]


def test_delete_cell_4():
    input_data = {
        'event': ["create", 'execute', 'create', 'create', 'delete'],
        'cell_index': ["c1", 'c2', 'c3', 'c4', 'c4'],
        'cell_num': [0, 5, 2, 6, 7],
        'cell_source': [None, "source_c2", None, None, None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)
    snap = processor.snapshots[-1]

    assert len(processor.snapshots) == 6
    assert snap.index_source_mapping == {"c1": None, "c2": "source_c2", "c3": None, "c4": None}
    assert snap.index_num_mapping == {"c1": 1, "c2": 6, "c3": 3}
    assert snap.index_order == [("c1", 1), ("c3", 3), ("c2", 6)]


def test_structure_size():
    input_data = {
        'event': ["create", 'execute', 'create', 'create', 'delete'],
        'cell_index': ["c1", 'c2', 'c3', 'c4', 'c4'],
        'cell_num': [0, 5, 2, 6, 7],
        'cell_source': [None, "source_c2", None, None, None]
    }
    test_df = pd.DataFrame(input_data).replace(np.nan, None)
    processor = SequenceProcessor(test_df)

    for snap in processor.snapshots:
        assert len(snap.index_num_mapping) == len(snap.index_order)

        if len(snap.index_order):
            order = np.array(snap.index_order).T[0]
            assert len(order) == len(set(order))


if __name__ == '__main__':
    test_create_cell()

    test_execute_known_cell()
    test_execute_unknown_cell()

    test_create_cell_2()
    test_create_cell_3()

    test_delete_cell()
    test_delete_cell_2()
    test_delete_cell_3()
    test_delete_cell_4()
    test_structure_size()

    print("SUCCESS")
