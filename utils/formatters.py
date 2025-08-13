import logging
import pandas as pd
from typing import List, Optional


def price_to_float(data_frame: pd.DataFrame, needed_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Converts specified columns in a DataFrame from string to float,
    using a robust method to clean up non-numeric characters.

    Args:
        data_frame (pd.DataFrame): The DataFrame to process.
        needed_columns (list, optional): A list of column names to convert. Defaults to None.

    Returns:
        pd.DataFrame: The DataFrame with the specified columns converted to float.
    """
    if needed_columns is None:
        logging.warning("No columns specified for price conversion. Returning original DataFrame.")
        return data_frame

    # Create a copy to avoid SettingWithCopyWarning
    df_copy = data_frame.copy()

    for column in needed_columns:
        if column in df_copy.columns:
            try:
                # Use a single regex to remove any character that is NOT a digit or a decimal point
                df_copy[column] = (
                    df_copy[column]
                    .astype(str)
                    .str.replace(r'[^\d.]', '', regex=True)
                )

                # Convert the cleaned string to a float
                df_copy[column] = pd.to_numeric(df_copy[column])

            except Exception as e:
                logging.error(f"Error converting column '{column}': {e}. Skipping this column.")
        else:
            logging.warning(f"Column '{column}' not found in DataFrame. Skipping.")

    return df_copy