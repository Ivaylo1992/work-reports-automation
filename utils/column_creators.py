from typing import Callable

import pandas as pd

from stock.stock import move_columns


def create_column(
        dataframe: pd.DataFrame,
        column_name: str,
        after_column_name: str = None,
        formula: Callable = None,
        formatter_func: Callable = None,
        *args,
        **kwargs
) -> pd.DataFrame:
    """
    Create a new column in a DataFrame, optionally calculated from a formula,
    and optionally reposition it after a specified column.

    Parameters
    ----------
    dataframe : pd.DataFrame
        The DataFrame in which to create the column.
    column_name : str
        The name of the new column to be created.
    after_column_name : str, optional
        If provided, the new column will be moved to appear immediately after
        this column in the DataFrame.
    formula : Callable, optional
        A function that computes the column values. It should accept the DataFrame
        as its first argument, followed by any additional arguments or keyword arguments.
    formatter_func: Callable, optional
        A function that takes a DataFrame as its first argument, and returns
        a formatted DataFrame as its second argument. Formats a given column.
    *args
        Positional arguments to pass to the formula.
    **kwargs
        Keyword arguments to pass to the formula.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the new column added (and repositioned if specified).

    Notes
    -----
    - If no formula is provided, the column will be filled with None values.
    - Relies on `move_columns` from `stock.stock` for column reordering.
    """
    value = None

    if formula:
        value = formula(dataframe, *args, **kwargs)

    dataframe[column_name] = value

    if formatter_func:
        dataframe = formatter_func(dataframe, column_name)

    if after_column_name:
        dataframe = move_columns(dataframe, after_column_name, columns_to_move=[column_name])

    return dataframe
