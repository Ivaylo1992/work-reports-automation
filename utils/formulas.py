from typing import Any

import pandas as pd
from pandas import Series


def markup(
        df: pd.DataFrame,
        cost_price_column_name: str ='PurchasePrice',
        sale_price_column_name: str = 'SalePrice',
        country: str = 'BG',
        round_to: int = 2,
):
    """
            Calculates the markup percentage and adds it as a new column to the DataFrame.

            Args:
                df (pd.DataFrame): Input data.
                cost_price_column_name (str): Name of the cost price column.
                sale_price_column_name (str): Name of the sale price column.
                country (str, optional): Country code used to apply VAT. Defaults to 'BG'.
                round_to (int, optional): Number of decimals to round the result to. Defaults to None (no rounding).

            Returns:
                Calculated markup .
        """

    vat_divisor = {'BG': 1.2, 'RO': 1.21, 'GR': 1.24, }

    if country not in vat_divisor.keys():
        raise ValueError(f"Country {country} is not supported.")

    required_columns = [cost_price_column_name, sale_price_column_name]

    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    mu = (
            df[sale_price_column_name] / vat_divisor[country] / df[cost_price_column_name]
    )

    mu = mu.round(round_to)

    return mu