import json
from ast import literal_eval
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import StrEnum

import numpy as np
import pandas as pd
from tqdm import tqdm


class JuNEDataset:
    def __init__(self, df):
        self.df_june = df
        self.df_states = None

    def prepare_dataset(self):
        self.df_june = self._preprocess_dataframe_columns(self.df_june)

    @property
    def df(self):
        return self.to_dataframe()

    def to_dataframe(self):
        return self.df_june

    @staticmethod
    def _preprocess_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:

        df = df.fillna(np.NaN).replace(np.NaN, None).iloc[:]

        # if 'expert' in list(df):
        #     df = df.drop('expert', axis=1).fillna(np.NaN).replace(np.NaN, None).iloc[:]

        df['time'] = pd.to_datetime(df['time'])
        df['time'] = df.time.apply(lambda x: datetime.fromtimestamp(datetime.timestamp(x)))
        df = df.sort_values(by=['time']).replace({np.nan: None})

        df['task'] = 'task2'
        df.loc[df.notebook_name.str.contains('task1'), 'task'] = 'task1'

        df['expert'] = False
        df.loc[df.user_id.str.contains('expert'), 'expert'] = True

        return df

    def to_evolution_dataframe(self, **kwargs) -> pd.DataFrame:
        if self.df_states is not None:
            return self.df_states

        kernel_df = self.df_june.groupby('kernel_id')

        kernel_dataframes = [
            self.get_kernel_states(kernel_id, **kwargs)
            for kernel_id in tqdm(self.df_june.kernel_id.unique())
        ]
        merged_df = pd.concat(kernel_dataframes)
        self.df_states = merged_df
        return merged_df

    def get_kernel_states(self, kernel_id: str, filter_state: bool = True) -> pd.DataFrame:
        kernel_df = self.df_june.groupby('kernel_id').get_group(kernel_id)

        state, state_tmp = NotebookState(), NotebookState()
        states = [state_tmp.to_dataframe()]

        for state_num, log_row in kernel_df.iterrows():
            action_id = log_row.action_id
            state_tmp = deepcopy(state)
            state_tmp.update_state(log_row)
            if filter_state:
                state_tmp = delete_duplicates(state_tmp)

            df = state_tmp.to_dataframe()
            df['action_id'] = action_id
            df['event'] = log_row.event

            states.append(df)
            state = state_tmp

        state_df = pd.concat(states)
        state_df['kernel_id'] = kernel_id

        return state_df

    def get_notebook_state_by_id(self, action_id: int) -> pd.DataFrame:
        df = self.to_evolution_dataframe()
        row = df.iloc[action_id]
        state_num = row.state_num
        kernel_id = row.kernel_id
        return (
            df.groupby('kernel_id').get_group(kernel_id).
            groupby("state_num").get_group(state_num)
        )

    @staticmethod
    def match_executions(cell_df):

        looking_for_finish = False
        found = {
            'executions': [],
            'unexecuted': [],
            'hagning_finish': []
        }

        cell_df['result'] = cell_df.cell_output.apply(
            lambda x: '' if (x is None) | (x == '[]') else '' + literal_eval(x)[0]['output_type'])

        for i, row in cell_df.iterrows():
            if row.event == 'execute':
                looking_for_finish = i
            if row.event == 'finished_execute':
                if looking_for_finish:
                    found['executions'].append(tuple([looking_for_finish, i]))
                    looking_for_finish = None
                else:
                    found['hagning_finish'].append(i)

        cell_df['execution_time'] = None
        cell_df['execution_result'] = 'ok'
        for execution in found['executions']:
            cell_df.loc[execution[0], 'execution_time'] = cell_df.loc[execution[1], 'time']
            cell_df.loc[execution[0], 'execution_result'] = cell_df.loc[execution[1], 'result']

        return cell_df

    @staticmethod
    def match_edits(cell_df: pd.DataFrame) -> pd.DataFrame:

        edit_state = None
        found = {
            'edited': [],
            'unedited': [],
            'uncreated': [],
        }

        for i, row in cell_df.iterrows():
            if (row.event == 'finished_execute') or (row.event == 'create'):
                edit_state = i
            if row.event == 'execute':
                if edit_state:
                    found['edited'].append(tuple([edit_state, i]))
                    edit_state = None
                else:
                    found['uncreated'].append(i)

        cell_df['edited_time'] = None
        for edited in found['edited']:
            cell_df.loc[edited[0], 'edited_time'] = cell_df.loc[edited[1], 'time']

        return cell_df


@dataclass
class Cell:
    cell_index: str
    cell_num: int
    cell_source: str = None


class ActionName(StrEnum):
    EXECUTE = "execute"
    RENDERED = "rendered"
    CREATE = "create"
    DELETE = "delete"
    SAVE = "save_notebook"


class NotebookState:
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
        self.log = dict()
        self.state_num = 0

    def display_notebook(self, cell_separator: str = "[CELL_SEPARATOR]") -> None:
        for cell in self.cells:
            print(f"[CELL INDEX {cell.cell_index}]\n", cell.cell_source)
            print(cell_separator)

    @property
    def cells(self):
        return [
            Cell(idx, self.index_num_mapping[idx], self.index_source_mapping[idx])
            for (idx, num) in self.index_order if
            idx in self.index_source_mapping.keys()
        ]

    def to_dataframe(self) -> pd.DataFrame:
        cells_dictionaries = [asdict(c) for c in self.cells]
        df = pd.DataFrame(cells_dictionaries)
        df['state_num'] = self.state_num

        for key, value in self.log.items():
            if key not in list(df):
                df[key] = value
        return df

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

    def update_state(self, log: pd.Series) -> None:
        action, cell_index, cell_num, cell_source = log.event, log.cell_index, log.cell_num, log.cell_source
        cell_num = int(cell_num) if cell_num is not None else cell_num
        self.log = log.to_dict()
        self.state_num += 1
        if action == "save_notebook":
            self.initialize_indices(cell_source)
            return

        if action != "error":
            self.index_source_mapping[cell_index] = cell_source

        cell = Cell(cell_index=cell_index, cell_num=cell_num)

        if action in self.actions_mapping:
            self.actions_mapping[action](cell)


def delete_duplicates(state: NotebookState) -> NotebookState:
    list_index_to_delete = []
    for i, (index_i, num_i) in enumerate(state.index_order):
        for j, (index_j, num_j) in enumerate(state.index_order):
            if j <= i:
                continue
            if num_i == num_j and index_i != index_j:
                list_index_to_delete.append(j)
                if index_j in state.index_num_mapping:
                    del state.index_num_mapping[index_j]

    state.index_order = [
        p for i, p in enumerate(state.index_order)
        if i not in set(list_index_to_delete)
    ]
    return state

#%%
