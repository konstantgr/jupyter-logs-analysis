import json
from pathlib import Path
from typing import Any

import docker
import pandas as pd

ROOT_PATH: Path = Path(__file__).resolve().parent.parent


class SafeDockerContainer:
    def __init__(self, params: dict):
        self.params = params
        self.client = None
        self.container = None

    def __enter__(self):
        self.client = docker.from_env()
        self.container = self.client.containers.run(**self.params)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.stop()
        self.container.remove()


def parse_json_or_empty(x: Any) -> dict:
    if pd.isna(x) or x == "":
        return {}
    try:
        return json.loads(x)
    except json.JSONDecodeError:
        return {}


def preprocess_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "cell_output"] = df["cell_output"].apply(parse_json_or_empty)
    df.loc[:, "cell_source"] = df.cell_source.apply(
        lambda x: "\n".join(
            [
                x_i
                for x_i in x.splitlines()
                if (("check_logging" not in x_i) and (not (x_i.startswith("!") or x_i.startswith("%")) or "pip" in x_i))
            ]
        )
        if not isinstance(x, float)
        else x
    )

    return df
