import os
from typing import Tuple

from dotenv import load_dotenv


def check_env_variables() -> None:
    _ = load_dotenv()
    needed_variables: Tuple[str, ...] = ("CHROMIUM_DRIVER_PATH",)
    for var in needed_variables:
        if os.environ.get(var) is None:
            raise RuntimeError(f"{var} environmental variable not declared")

    print("All variables declared")


if __name__ == "__main__":
    check_env_variables()
