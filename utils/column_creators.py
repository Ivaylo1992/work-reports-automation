from typing import Callable

import pandas as pd

from stock.stock import move_columns


def create_column(
        dataframe: pd.DataFrame,
        column_name: str,
        after_column_name: str = None,
        formula: Callable = None,
        *args,
        **kwargs
) -> pd.DataFrame:
    value = None

    if formula:
        value = formula(dataframe, *args, **kwargs)

    dataframe[column_name] = value

    if after_column_name:
        dataframe = move_columns(dataframe, after_column_name, columns_to_move=[column_name])

    return dataframe