import logging
from typing import Optional, List

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_stock_report(
        input_file_path: str,
        output_file_path: str,
        concept_filter: str = "OUTLET",
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

# process_stock_report(
#     input_file_path='data/Available Stock Report.xlsx',
#     output_file_path='data/new_report_basic.xlsx'
# )


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

        pivot_table.to_excel(output_file_path)
        logging.info(f"Successfully created pivot table in {output_file_path}")

    except KeyError as e:
        logging.error(f"Error: Column not found during pivot table creation: {e}. "
                      f"Check 'index_cols', 'value_column', or 'column_to_pivot'.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during pivot table creation: {e}")


# Example 1: Basic usage with default arguments
# create_pivot_table(
#     input_file_path='data/new_report_basic.xlsx',
#     output_file_path='data/pivot_table_basic.xlsx',
# )

# Example 2: Custom index columns and aggregation column
# create_pivot_table(
#     input_file_path='data/new_report_basic.xlsx',
#     output_file_path='data/pivot_table_custom.xlsx',
#     index_cols=['Brand', 'Category'],
#     value_column='RESERVED', # Assuming 'RESERVED' is in the new_report_basic.xlsx
#     agg_func='mean'
# )

# Example 3: Demonstrate missing column error
# create_pivot_table(
#     input_file_path='data/new_report_basic.xlsx',
#     output_file_path='data/pivot_table_error.xlsx',
#     index_cols=['NonExistentCol'], # This will trigger an error
# )

def merge_tables(
        prices_table_path: str,
        stock_table_path: str,
        output_file_path: str,
        needed_price_columns: Optional[List[str]] = None,
        merge_on: str = None,
) -> None:
    stock_table = pd.read_excel(stock_table_path)
    prices_df = pd.read_excel(prices_table_path)

    if not needed_price_columns:
        needed_price_columns = ['SKU_CODE', 'SalePrices', 'InitialPrice', 'PurchasePrice']

    if not merge_on:
        merge_on = 'SKU_CODE'

    try:
        merged_df = pd.merge(
            stock_table,
            prices_df[needed_price_columns],
            on=merge_on,
            how='left',
        )

        merged_df.to_excel(output_file_path)
    except KeyError as e:
        logging.error(f"Error: Column not found during merge table creation: {e}. ")


merge_tables(
    "data/clean_prices_basic.xlsx",
    "data/new_report_basic.xlsx",
    "data/merged.xlsx"
)





