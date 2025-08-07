import logging
import pandas as pd

def clean_prices_table(
        input_file_path: str,
        output_file_path: str,
        plant: int = 4315,
        columns_to_rename: dict = None,
        excel_header_row: int = 0,
    ) -> None:
    """
    Cleans the prices table by filtering for a specific plant and renaming columns.

    Args:
        input_file_path (str): Path to the raw Excel file.
        output_file_path (str): Path to save the cleaned Excel file.
        plant (int): Plant code to filter on (default: 4315).
        columns_to_rename (dict): Columns to rename, e.g., {'Material': 'SKU_CODE'}.
        excel_header_row (int): Header row index (default: 0).
    """
    if columns_to_rename is None:
        columns_to_rename = {'Material': 'SKU_CODE'}

    try:
        prices_data = pd.read_excel(input_file_path, header=excel_header_row)
        logging.info(f'Successfully read {input_file_path}.')

        required_columns = ['Plant'] + list(columns_to_rename.keys())
        missing = [col for col in required_columns if col not in prices_data.columns]
        if missing:
            logging.error(f"Missing expected columns: {missing}")
            return

        prices_data = prices_data[prices_data['Plant'] == plant].reset_index(drop=True)
        prices_data.rename(columns=columns_to_rename, inplace=True)

        prices_data.to_excel(output_file_path, index=False)
        logging.info(f'Successfully created {output_file_path}.')

    except FileNotFoundError:
        logging.error(f'File {input_file_path} not found.')
    except Exception as e:
        logging.error(f'Unexpected error occurred: {e}.')
