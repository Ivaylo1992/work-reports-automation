import pandas as pd
import logging
from typing import Dict, Optional

def clean_prices_table(
    df: pd.DataFrame,
    plant: int = 4315,
    columns_to_rename: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Cleans the prices table DataFrame by filtering for a specific plant and renaming columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing price data.
        plant (int): Plant code to filter on. Defaults to 4315.
        columns_to_rename (dict, optional): Mapping of columns to rename.
                                            Defaults to {'Material': 'SKU_CODE'}.

    Returns:
        pd.DataFrame: Cleaned DataFrame.

    Raises:
        ValueError: If required columns are missing.
    """
    if columns_to_rename is None:
        columns_to_rename = {'Material': 'SKU_CODE'}

    # Check required columns exist
    required_columns = ['Plant'] + list(columns_to_rename.keys())
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logging.error(f"Missing expected columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    try:
        # Filter and rename
        cleaned_df = (
            df[df['Plant'] == plant]
            .reset_index(drop=True)
            .rename(columns=columns_to_rename)
        )

        logging.info("Successfully cleaned prices table DataFrame.")
        return cleaned_df

    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        raise
