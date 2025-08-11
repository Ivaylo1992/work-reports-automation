import pandas as pd


def create_markup(
        dataframe: pd.DataFrame,
        cost_price_column_name: str ='PurchasePrice',
        sale_price_column_name: str = 'SalePrice',
        round_to: int = 2,
        country: str = 'BG',
        column_name: str = 'Markup',
) -> pd.DataFrame:
    """
        Calculates the markup percentage and adds it as a new column to the DataFrame.

        Args:
            dataframe (pd.DataFrame): Input data.
            cost_price_column_name (str): Name of the cost price column.
            sale_price_column_name (str): Name of the sale price column.
            country (str, optional): Country code used to apply VAT. Defaults to 'BG'.
            column_name (str, optional): Name of the new column to store markup. Defaults to 'Markup'.
            round_to (int, optional): Number of decimals to round the result to. Defaults to None (no rounding).

        Returns:
            pd.DataFrame: The original DataFrame with a new 'Markup' column.
    """

    vat_divisor = {'BG': 1.2, 'RO': 1.21,'GR': 1.24,}

    if country not in vat_divisor.keys():
        raise ValueError(f"Country {country} is not supported.")

    required_columns = [cost_price_column_name, sale_price_column_name]

    missing_columns = [c for c in required_columns if c not in dataframe.columns]

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    markup = (
            dataframe[sale_price_column_name] / vat_divisor[country] / dataframe[cost_price_column_name]
    )

    markup = markup.round(round_to)

    dataframe[column_name] = markup

    return dataframe