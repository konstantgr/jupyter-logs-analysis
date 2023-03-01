import pandas as pd
import numpy as np

from typing import List
from pathlib import Path
from sqlalchemy import create_engine


def get_databases(data_path: Path) -> List[Path]:
    return list(data_path.rglob("*.db"))


def get_group(df: pd.DataFrame, feature: str, group_name: str) -> pd.DataFrame:
    return df.groupby(feature).get_group(group_name)


def load_data(db_path: Path) -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{db_path}")

    with engine.connect() as connection:
        df = pd.read_sql_table(
            "user_logs",
            con=connection
        )

    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by=['time']).replace({np.nan: None})

    return df


if __name__ == '__main__':
    print(list(Path('../data').rglob("*.db")))
