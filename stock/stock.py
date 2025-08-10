import logging
from typing import Optional, List, Callable, Union
import pandas as pd

from utils.formulas import calculate_markup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_stock_report_df(
    df: pd.DataFrame,
    concept_filter: str = None,
    columns_to_drop: Optional[List[str]] = None,
    columns_to_format_as_int: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Processes a stock report DataFrame: filters by concept,
    drops specified columns, formats columns to integer, and returns the result.

    Args:
        df (pd.DataFrame): Input DataFrame containing the stock report.
        concept_filter (str): The value in the 'Concept' column to filter by.
                              Defaults to 'OUTLET'.
        columns_to_drop (list): A list of column names to drop.
                                Defaults to ['STOCK_UPDATE', 'SIZE', 'Subcategory',
                                             'Licence', 'Barcode', 'STOCK_WITHOUT_REZERVED',
                                             'REZERVED'].
        columns_to_format_as_int (list): A list of column names to convert to integer type.
                                         Defaults to ['AVAILABLE'].

    Returns:
        pd.DataFrame: Processed DataFrame.
    """

    if concept_filter is None:
        concept_filter = 'OUTLET'

    if columns_to_drop is None:
        columns_to_drop = [
            'STOCK_UPDATE',
            'SIZE',
            'Subcategory',
            'Licence',
            'Barcode',
            'STOCK_WITHOUT_REZERVED',
            'REZERVED'
        ]

    if columns_to_format_as_int is None:
        columns_to_format_as_int = ['AVAILABLE']

    # Create formatting operations for .assign()
    format_assignments = {
        col: (lambda x, c=col: x[c].astype(int)) for col in columns_to_format_as_int
    }

    try:
        df_processed = (
            df.drop(columns=[col for col in columns_to_drop if col in df.columns])
              .query(f'Concept == "{concept_filter}"')
              .reset_index(drop=True)
              .assign(**format_assignments)
        )

        logging.info(f"Successfully processed DataFrame. Columns: {df_processed.columns}")
        return df_processed

    except Exception as e:
        logging.error(f"An error occurred during DataFrame processing: {e}")
        raise


def create_pivot_table_df(
    df: pd.DataFrame,
    index_cols: Optional[List[str]] = None,
    value_column: str = 'AVAILABLE',
    column_to_pivot: str = 'STORE_CODE',
    agg_func: Union[str, callable] = 'sum'
) -> pd.DataFrame:
    """
    Creates a pivot table from a DataFrame by aggregating data.

    Args:
        df (pd.DataFrame): Input DataFrame.
        index_cols (list, optional): A list of column names to use as the pivot table index.
                                     Defaults to ['SKU_CODE', 'SKU_DESCRIPTION', 'Brand',
                                                  'Category', 'Activity', 'Gen', 'Subgen'].
        value_column (str, optional): The column to aggregate. Defaults to 'AVAILABLE'.
        column_to_pivot (str, optional): The column whose unique values become new columns.
                                         Defaults to 'STORE_CODE'.
        agg_func (str or callable, optional): Aggregation function. Defaults to 'sum'.

    Returns:
        pd.DataFrame: Pivot table DataFrame.
    """

    if index_cols is None:
        index_cols = [
            'SKU_CODE', 'SKU_DESCRIPTION', 'Brand', 'Category',
            'Activity', 'Gen', 'Subgen'
        ]

    # Validate required columns
    required_columns = index_cols + [value_column, column_to_pivot]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.error(f"Missing required columns in DataFrame: {missing_columns}")
        logging.info(f"Available columns: {list(df.columns)}")
        raise ValueError(f"Missing required columns: {missing_columns}")

    try:
        pivot_table = (
            df.groupby(index_cols + [column_to_pivot])[value_column]
            .agg(agg_func)
            .unstack(level=column_to_pivot, fill_value=0)
        )

        # Remove column axis name if present
        if column_to_pivot in pivot_table.columns.names:
            pivot_table = pivot_table.rename_axis(columns=None)

        logging.info("Successfully created pivot table.")
        return pivot_table

    except KeyError as e:
        logging.error(f"Column not found during pivot table creation: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during pivot table creation: {e}")
        raise


def merge_tables(
    prices_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    needed_price_columns: Optional[List[str]] = None,
    merge_on: Optional[str] = None
) -> pd.DataFrame:
    """
    Merges a price DataFrame with a stock DataFrame on a common column.

    Args:
        prices_df (pd.DataFrame): DataFrame containing price data.
        stock_df (pd.DataFrame): DataFrame containing stock data.
        needed_price_columns (List[str], optional): Columns to keep from the price DataFrame.
                                                    Defaults to ['SKU_CODE', 'SalePrice', 'InitialPrice', 'PurchasePrice'].
        merge_on (str, optional): Column to merge on. Defaults to 'SKU_CODE'.

    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    if not needed_price_columns:
        needed_price_columns = ['SKU_CODE', 'SalePrice', 'InitialPrice', 'PurchasePrice']

    if not merge_on:
        merge_on = 'SKU_CODE'

    missing_columns = [c for c in needed_price_columns if c not in prices_df.columns]
    if missing_columns:
        logging.error(f"Missing required columns in price DataFrame: {missing_columns}")
        raise ValueError(f"Missing columns in prices_df: {missing_columns}")

    try:
        merged_df = pd.merge(
            stock_df,
            prices_df[needed_price_columns],
            on=merge_on,
            how='left'
        )
        logging.info("Successfully merged DataFrames.")
        return merged_df

    except KeyError as e:
        logging.error(f"Column not found during merge: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during merge: {e}")
        raise


def move_columns(
    df: pd.DataFrame,
    after_column: str,
    columns_to_move: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Moves specified columns in a DataFrame to appear immediately after a given column.

    Args:
        df (pd.DataFrame): Input DataFrame.
        after_column (str): The column after which the specified columns will be inserted.
        columns_to_move (List[str], optional): List of column names to move.
                                               Defaults to ['SalePrice', 'InitialPrice', 'PurchasePrice'].

    Returns:
        pd.DataFrame: DataFrame with columns rearranged.
    """
    if not columns_to_move:
        columns_to_move = ['SalePrice', 'InitialPrice', 'PurchasePrice']

    if after_column not in df.columns:
        logging.error(f"Column {after_column} not found in DataFrame.")
        raise ValueError(f"Missing after_column: {after_column}")

    missing_columns = [c for c in columns_to_move if c not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns to move: {missing_columns}")
        raise ValueError(f"Missing columns_to_move: {missing_columns}")

    # Keep only columns that are not being moved
    columns = [col for col in df.columns if col not in columns_to_move]

    # Find insertion index
    insert_at = columns.index(after_column) + 1

    # Create new column order
    new_columns = columns[:insert_at] + columns_to_move + columns[insert_at:]

    logging.info("Successfully moved columns.")
    return df.reindex(columns=new_columns)


def add_column(
    input_file_path: str,
    output_file_path: str,
    column_name: str,
    after_column: str = None,
    calculation_formula: Callable = None,
):
    df = pd.read_excel(input_file_path)
    logging.info(f'Successfully read {input_file_path}.')

    if calculation_formula:
        df = calculation_formula(df, column_name)

    df.to_excel(output_file_path, index=False)
    logging.info(f"Saved to {output_file_path}.")

    if after_column:
        df = move_columns(input_file_path, output_file_path, after_column, columns_to_move=[column_name])
        df.to_excel(output_file_path, index=False)
        logging.info(f"Columns moved. Output saved to {output_file_path}.")


move_columns('../data/new_merged_prices_basic.xlsx', '../data/new_stock.xlsx', 'Subgen')
add_column('../data/new_stock.xlsx', '../data/new_stock.xlsx', 'Mkp', calculation_formula=calculate_markup)
