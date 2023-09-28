# How Are Notebooks Actually Developed?

# A Fine-Grained Analysis of Jupyter Notebooks Execution Logs

## Setup Environment

To get started, follow these steps to set up your environment:

1. Create a new virtual environment using Poetry:

  ```shell
  poetry shell
  ```

2. Install the necessary packages:

```shell
poetry install
```

## Repository Structure

This repository is organized as follows:

- `server/`: This folder contains scripts for creating an SQLite database and running a remote server for collecting
  logs.
  To start the server, simply use the following command in your terminal:

```shell
poetry run run_server
```

- `analysis/` In this directory, you'll find post-processing scripts and Jupyter notebooks for in-depth analysis.
    - `analysis/dataset/`: This folder contains scripts for preprocessing raw data into the JuNE dataset.
    - `analysis/metrics/`: Here, you'll find scripts for processing the JuNE dataset to extract various metrics, such as
      graph
      metrics, time metrics, transition metrics, and more.
- `data/`  All the necessary data required for reproducibility of our analysis is stored in this folder. You'll find
  datasets with logs, sample overviews, and other relevant data here.