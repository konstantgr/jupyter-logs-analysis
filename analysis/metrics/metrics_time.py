from ast import literal_eval
import pandas as pd
from .metrics_base import Metrics

class TimeMetrics(Metrics):

    def __init__(self):
        pass

    def calculate_metrics(self, df) -> pd.DataFrame:
        time_df = df.sort_values(['time', 'cell_index'])
        time_df = time_df.loc[time_df.event.isin(['execute','create', 'finished_execute','delete']), :]
        
        executions_matched = time_df.groupby('cell_index', group_keys=True).apply(self.match_executions)
        executions_matched = executions_matched.reset_index(drop=True)
        
        executions_matched = executions_matched.groupby('cell_index', group_keys=True).apply(self.match_edits)
        executions_matched = executions_matched.reset_index(drop=True)

        executions_matched = executions_matched.sort_values(['time', 'cell_index'])
        executions_matched['state_time'] = executions_matched.execution_time.\
            combine_first(executions_matched.edited_time)

        return executions_matched

    @staticmethod
    def match_executions(cell_df):

        looking_for_finish = False
        found = {
            'executions':[],
            'unexecuted':[],
            'hagning_finish':[]
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
        # cell_test = cell_test.copy(deep=True)
        for execution in found['executions']:
            cell_df.loc[execution[0], 'execution_time'] = cell_df.loc[execution[1], 'time']
            cell_df.loc[execution[0], 'execution_result'] = cell_df.loc[execution[1], 'result']

        # if len(found['unexecuted']) > 0:
        #     print(f'{cell_df.cell_index.iloc[0]} found unfinished executions')

        # if len(found['hagning_finish']) > 0:
        #     print(f' {cell_df.cell_index.iloc[0]} found hagning finish')

        return cell_df

    @staticmethod
    def match_edits(cell_df):

        edit_state = None
        found = {
            'edited':[],
            'unedited':[],
            'uncreated':[],
        }

        for i, row in cell_df.iterrows():
            if ((row.event == 'finished_execute') or (row.event == 'create')):
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