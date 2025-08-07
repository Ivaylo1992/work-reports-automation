import logging
from typing import Optional, List

import pandas as pd

from prices import clean_prices_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_stock_report(
        input_file_path: str,
        output_file_path: str,
        concept_filter: str = None,
        columns_to_drop: Optional[List[str]] = None,
        columns_to_format_as_int: Optional[List[str]] = None,
        excel_header_row: int = 2,
    ) -> None:
        """
        Processes an available stock report, filters by concept,
        drops specified columns, formats columns to integer, and saves to a new Excel file.

        Args:
        input_file_path (str): Path to the input Excel file.
        output_file_path (str): Path to save the processed Excel file.
        concept_filter (str): The value in the 'Concept' column to filter by. Defaults to 'OUTLET'.
        columns_to_drop (list): A list of column names to drop. Defaults to original list.
        columns_to_format_as_int (list): A list of column names to convert to integer type.
                                          Defaults to ['AVAILABLE'].
        excel_header_row (int): The 0-indexed row number to use as the header. Defaults to 2.
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

        # Dynamically create the dictionary for .assign()
        format_assignments = {
          col: lambda x, c=col: x[c].astype(int) for col in columns_to_format_as_int
        }

        try:
            df_processed = (
              pd.read_excel(input_file_path, header=excel_header_row)
              .drop(columns=columns_to_drop)
              .query(f'Concept == "{concept_filter}"')
              .reset_index(drop=True)
              .assign(**format_assignments)
            )

            df_processed.to_excel(output_file_path, index=False)
        except FileNotFoundError:
            logging.error(f'File "{input_file_path}" not found.')
        except Exception as e:
            logging.error(f"An unexpected error occurred during pivot table creation: {e}")

        logging.info(f"Saved to {output_file_path}")


def create_pivot_table(
        input_file_path: str,
        output_file_path: str,
        index_cols: Optional[List[str]] = None,
        value_column: str = 'AVAILABLE',
        column_to_pivot: str = 'STORE_CODE',
        agg_func: str = 'sum',
        excel_header_row: int = 0,
):
    """
    Creates a pivot table from an Excel file, aggregates data and saves it.

    Args:
        input_file_path (str): Path to the input Excel file.
        output_file_path (str): Path to save the generated pivot table Excel file.
        index_cols (list, optional): A list of column names to use as the pivot table index. Defaults to common SKU/product attributes.
        value_column (str, optional): The name of the column to aggregate (the 'values' in pivot_table).Defaults to 'AVAILABLE'.
        column_to_pivot (str, optional): The name of the column whose unique values will become new columns.Defaults to 'STORE_CODE'.
        agg_func (str, optional): The aggregation function to apply (e.g., 'sum', 'mean', 'count').Defaults to 'sum'.
        excel_header_row (int, optional): The 0-indexed row number to use as the header when reading. Defaults to 0, assuming clean data.
    """

    try:
        df = pd.read_excel(input_file_path, header=excel_header_row)
        logging.info(f"Successfully read {input_file_path}. Columns: {df.columns}")
    except FileNotFoundError:
        logging.error(f"File {input_file_path} not found.")
        return
    except Exception as e:
        logging.error(f"Error reading Excel file {input_file_path}: {e}")
        return

    if index_cols is None:
        index_cols = [
            'SKU_CODE', 'SKU_DESCRIPTION', 'Brand', 'Category',
            'Activity', 'Gen', 'Subgen'
        ]

    # Validate that all required columns exist before proceeding
    required_columns = index_cols + [value_column, column_to_pivot]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.error(f"Error: Missing required columns in input file: {missing_columns}")
        logging.info(f"Available columns: {list(df.columns)}")
        return

    try:
        pivot_table = (
            df.groupby(index_cols + [column_to_pivot])
            [value_column]
            .agg(agg_func)
            .unstack(level=column_to_pivot, fill_value=0)
        )

        if column_to_pivot in pivot_table.columns.names:
            pivot_table = pivot_table.rename_axis(columns=None)

        pivot_table.to_excel(output_file_path, index=True)
        logging.info(f"Successfully created pivot table in {output_file_path}")

    except KeyError as e:
        logging.error(f"Error: Column not found during pivot table creation: {e}. "
                      f"Check 'index_cols', 'value_column', or 'column_to_pivot'.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during pivot table creation: {e}")


def merge_tables(
        prices_table_path: str,
        stock_table_path: str,
        output_file_path: str,
        needed_price_columns: Optional[List[str]] = None,
        merge_on: str = None,
) -> None:
    """
    Merges a price table with a stock table on a common column and saves the result.

    Args:
        prices_table_path (str): Path to the prices Excel file.
        stock_table_path (str): Path to the stock Excel file.
        output_file_path (str): Path to save the merged result.
        needed_price_columns (List[str], optional): Columns to keep from the price table.
        merge_on (str): Column to merge on.

    Returns:
        None on failure.
    """
    stock_table = pd.read_excel(stock_table_path)
    prices_df = pd.read_excel(prices_table_path)

    if not needed_price_columns:
        needed_price_columns = ['SKU_CODE', 'SalePrice', 'InitialPrice', 'PurchasePrice']

    if not merge_on:
        merge_on = 'SKU_CODE'

    missing_columns = [c for c in needed_price_columns if c not in prices_df.columns]

    if missing_columns:
        logging.error(f"Error: Missing required columns in input file: {missing_columns}")
        return

    try:
        merged_df = pd.merge(
            stock_table,
            prices_df[needed_price_columns],
            on=merge_on,
            how='left',
        )

        merged_df.to_excel(output_file_path, index=False)
        logging.info(f"Successfully merged {output_file_path}.")

    except KeyError as e:
        logging.error(f"Error: Column not found during merge table creation: {e}. ")

    except Exception as e:
        logging.error(f"Unexpected error during merge: {e}")
        return None


# process_stock_report(
#     'data/Available Stock Report.xlsx',
#     'data/new_stock_report.xlsx',
# )

# create_pivot_table(
#     'data/new_stock_report.xlsx',
#     'data/new_pivot_table.xlsx',
# )
#
# clean_prices_table(
#     'data/prices.xlsx',
#     'data/new_prices.xlsx',
#     excel_header_row=2,
# )

#
# merge_tables(
#     'data/new_prices.xlsx',
#     'data/new_pivot_table.xlsx',
#     'data/new_merged_prices_basic.xlsx',
# )


