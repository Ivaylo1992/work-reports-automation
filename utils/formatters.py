from typing import List, Optional

import pandas as pd


def price_to_float(data_frame: pd.DataFrame, needed_columns: Optional[List[str]] = None) -> pd.DataFrame:
    for column in needed_columns:
        if column in data_frame.columns:
            data_frame[column] = data_frame[column].astype(float)

    return data_frame