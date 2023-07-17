from datetime import datetime
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
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
    df['time'] = df.time.apply(lambda x: datetime.fromtimestamp(datetime.timestamp(x)))
    df = df.sort_values(by=['time']).replace({np.nan: None})

    return df


if __name__ == '__main__':
    print(list(Path('../data').rglob("*.db")))
