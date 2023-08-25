import sqlite3
from pathlib import Path

import pandas as pd
import yaml


def read_config(config_path: Path) -> dict:
    with config_path.open("r") as stream:
        return yaml.safe_load(stream)


def get_users_kernels(config_path: Path) -> pd.DataFrame:
    config = read_config(config_path)
    users_kernels_mapping = {
        **{f'student_{i}': kernel_list for i, kernel_list in enumerate(config['students'])},
        **{f'expert_{i}': kernel_list for i, kernel_list in enumerate(config['experts'])},
    }
    users = pd.concat({k: pd.Series(v) for k, v in users_kernels_mapping.items()})\
        .reset_index().drop(['level_1'],
                                                                                                      axis=1)
    users.columns = ['user_id', 'kernel_id']
    return users


def get_kernel_actions(df: pd.DataFrame, kernel_id: str) -> pd.DataFrame:
    return df.groupby('kernel_id').get_group(kernel_id)


def read_hackathon_data(config_path: Path, attach_users: bool = True) -> pd.DataFrame:
    config = read_config(config_path)
    query = "SELECT * FROM user_logs"
    base_path = Path(config['base_path'])

    dataframes = []
    for db_path in config['student_path']:
        print(db_path)
        df_tmp = pd.read_sql_query(query, sqlite3.connect(base_path / db_path))
        df_tmp['expert'] = False
        dataframes.append(df_tmp)

    for db_path in config['expert_path']:
        df_tmp = pd.read_sql_query(query, sqlite3.connect(base_path / db_path))
        df_tmp['expert'] = True
        dataframes.append(df_tmp)

    df = pd.concat(dataframes)

    if attach_users:
        users = get_users_kernels(config_path).dropna(subset=["kernel_id"])
        df = df.merge(users, on='kernel_id')

    return df


if __name__ == '__main__':
    path = Path("data_config.yaml")
    df_hack = read_hackathon_data(path)
    print(df_hack.shape, df_hack.dropna(subset=["user_id"]).shape, df_hack.user_id.unique())
