import pandas as pd
import numpy as np
from ast import literal_eval


class JuNEDataset:

    def __init__(self, df):
        self.df_june = df

    def prepare_dataset(self):
        self.df_june = self._preprocess_dataframe_columns(self.df_june)

    def as_df(self):
        return self.df_june

    @staticmethod
    def _preprocess_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
        if 'expert' in list(df):
            return df.drop('expert', axis=1).fillna(np.NaN).replace(np.NaN, None).iloc[:]



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
    def match_edits(cell_df):

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