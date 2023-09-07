from ast import literal_eval

import pandas as pd
import numpy as np

from .metrics_base import Metrics


class TimeMetrics(Metrics):

    def __init__(self):
        self.unexpected_finish = []
        self.unfinished = []

    def calculate_metrics(self, df) -> pd.DataFrame:

        time_df = df.sort_values(['time', 'cell_index'])
        time_df = time_df.loc[time_df.event.isin(['execute', 'create', 'finished_execute', 'delete']), :]
        time_df.time = pd.to_datetime(time_df.time)

        execution_times = time_df.groupby('kernel_id').apply(self.match_executions)
        time_df = time_df.merge(execution_times, on=['action_id', 'cell_index'], how="left")

        time_df = time_df.groupby('kernel_id').apply(self.calculate_interruptions).reset_index(drop=True)
        time_df['src_len'] = time_df.cell_source.str.len()
        time_df['execution_time_sec'] = time_df.execution_time.dt.total_seconds()


        # executions_matched = time_df.groupby('cell_index', group_keys=True).apply(self.match_executions)
        # executions_matched = executions_matched.reset_index(drop=True)
        #
        # executions_matched = executions_matched.groupby('cell_index', group_keys=True).apply(self.match_edits)
        # executions_matched = executions_matched.reset_index(drop=True)

        # executions_matched = executions_matched.sort_values(['time', 'cell_index'])
        # executions_matched['state_time'] = executions_matched.execution_time. \
        #     combine_first(executions_matched.edited_time)


        # time_df.state_time = pd.to_datetime(time_df.state_time)

        time_df = self.calculate_next_action_time(time_df)

        # executions_matched['state_time_dt'] = pd.to_datetime(executions_matched.state_time) - executions_matched.time
        # executions_matched.state_time_dt = executions_matched.state_time_dt.dt.total_seconds()

        # executions_matched = self.calculate_interruptions(executions_matched)

        return time_df

    @staticmethod
    def calculate_next_action_time(metrics):
        df_tmp = metrics.groupby('kernel_id').apply(lambda x: (x.time - x.time.shift(1)).dt.total_seconds().shift(-1))\
            .reset_index(level=0)\
            .drop(columns=['kernel_id'])
        df_tmp.columns = ['next_action_time']
        metrics = metrics.join(df_tmp)
        return metrics

    @staticmethod
    def calculate_interruptions(metric_df):

        metric_df['interruptions'] = 0
        for i, row in metric_df.iterrows():
            if row.execution_start is not None:
                metric_df.loc[i, 'interruptions'] = sum(metric_df.time.between(
                    row.execution_start, row.execution_start+row.execution_time, inclusive='neither'))

        return metric_df


    @staticmethod
    def parse_result(raw_output):
        result = '' if (raw_output is None) | (raw_output == '[]') else '' + literal_eval(raw_output)[0]['output_type']
        return result

    def match_executions(self, kernel_df):

        kernel_df = kernel_df.sort_values(by='time')
        all_executions = []
        execution_queue = []

        for i, row in kernel_df.iterrows():
            if row.event == 'execute':
                # we collect queue of cells
                if len(execution_queue) == 0:
                    # we set starting execution time at the start of queue
                    latest_time = row.time
                # fill the queue

                execution_queue.append((row.cell_index, row['action_id'], row.time, None, None))

            if row.event == 'finished_execute':
                result = self.parse_result(row.cell_output)
                # if we encounter finish with empty queue do something
                if len(execution_queue) == 0:
                    self.unexpected_finish.append((row.cell_index, row['action_id'], row.time, result))

                # if we encounter finish we check if it is in queue
                if row.cell_index in [n[0] for n in execution_queue]:
                    # we look for the latest encounter of cell in our queue
                    latest = max(i for i, n in enumerate(execution_queue) if n[0] == row.cell_index)
                    # we calculate execution time (current time - time of the last queue input)

                    # TODO: normal fix?
                    latest_time = execution_queue[latest][2] if execution_queue[latest][2] > latest_time else latest_time

                    execution_time = row.time - latest_time
                    # write all information
                    register_execution = (execution_queue[latest][1], row.cell_index,
                                          execution_time, latest_time, result)
                    all_executions.append(register_execution)
                    # update latest time -- now we start next calculation in queue from this point
                    latest_time = row.time
                    # remove entry from queue
                    del execution_queue[latest]
                # finish cell and not in queue - do something
                else:
                    self.unexpected_finish.append((row.cell_index, row['action_id'], row.time, result))

        if len(execution_queue) != 0:
            self.unfinished.append(execution_queue)

        return pd.DataFrame(all_executions, columns=['action_id', 'cell_index', 'execution_time',
                                                     'execution_start', 'result'])


    @staticmethod
    def match_edits(cell_df):

        edit_state = None
        found = {
            'edited': [],
            'unedited': [],
            'uncreated': [],
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

#%%
