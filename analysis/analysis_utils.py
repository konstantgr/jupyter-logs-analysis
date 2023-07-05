import yaml
import sqlite3
import pandas as pd

from pathlib import Path


with Path("data_config.yaml").open("r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        exit()


def read_hackathon_data():
    query = "SELECT * FROM user_logs"
    base_path = Path(config['base_path'])

    dataframes = []
    for db_path in config['student_path']:
        df_tmp = pd.read_sql_query(query, sqlite3.connect(base_path / db_path))
        df_tmp['expert'] = False
        dataframes.append(df_tmp)

    for db_path in config['expert_path']:
        df_tmp = pd.read_sql_query(query, sqlite3.connect(base_path / db_path))
        df_tmp['expert'] = True
        dataframes.append(df_tmp)

    return pd.concat(dataframes)


def get_sorted_kernels(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        df.groupby('kernel_id').size().sort_values(ascending=False).reset_index()
    )


def get_kernel_actions(df: pd.DataFrame, kernel_id: str) -> pd.DataFrame:
    return df.groupby('kernel_id').get_group(kernel_id)


if __name__ == '__main__':
    df_hack = read_hackathon_data()
