import os
from pathlib import Path

import click
import pandas as pd
from dotenv import load_dotenv

from notebook_reproduction import ROOT_PATH, SafeDockerContainer, preprocess_dataset
from notebook_reproduction.notebook_runner.runner import NotebookRunner
from notebook_reproduction.notebook_runner.selenium_notebook import SeleniumNotebook

env = load_dotenv()


@click.command()
@click.option("--kernel_id", prompt="Kernel ID", help="Kernel ID to use for analysis.")
@click.option("--notebook_port", default=2222, help="Jupyter notebook port")
@click.option("--save", default=True, help="Path to save the processed dataset.")
def main(kernel_id: str, notebook_port: int, save: bool):
    selenium_notebook = SeleniumNotebook(
        Path("test_notebook.ipynb"),
        Path(os.environ["CHROMIUM_DRIVER_PATH"]),
        Path(f"http://localhost:{notebook_port}"),
    )

    docker_params = {
        "image": "agent-jupyter-interaction-docker-image",
        "ports": {"8888/tcp": notebook_port},
        "detach": True,
    }
    df = pd.read_csv(ROOT_PATH / "data/local_data/dataset.csv", index_col=0)
    df = preprocess_dataset(df)
    df_sample = df[df.kernel_id == kernel_id]

    # bad_events = get_bad_events()
    # if bad_events.get(kernel_id):
    #     df_sample = df_sample.drop(df_sample.index[bad_events[kernel_id]])

    container = SafeDockerContainer(docker_params)
    with container, selenium_notebook as notebook:
        runner = NotebookRunner(dataset=df_sample, notebook=notebook)
        runner.reproduce()


if __name__ == "__main__":
    main()
