import pandas as pd


def get_sorted_kernels(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        df.groupby('kernel_id').size().sort_values(ascending=False).reset_index()
    )


def get_kernel_actions(df: pd.DataFrame, kernel_id: str) -> pd.DataFrame:
    return df.groupby('kernel_id').get_group(kernel_id)

#
# def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
#     df = df.dropna()
#     df['ccn'] = df.cell_source.apply(lambda x: get_complexity(x))
#     df['sloc'] = df.cell_source.apply(lambda x: get_sloc(x))
#     df['len'] = df.cell_source.apply(lambda x: len(x))
#     df['objects_num'] = df.cell_source.apply(lambda x: len(get_objects(x)))
#     df = df.dropna()


