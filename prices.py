import pandas as pd

def clean_prices_table(
        input_file_path: str,
        output_file_path: str,
        plant: str = None,
        columns_to_rename: dict = None,
        excel_header_row: int = 0,
    ) -> None:

    if plant is None:
        plant = 4315

    if columns_to_rename is None:
        columns_to_rename = {'Material': 'SKU_CODE'}


    prices_data = pd.read_excel(input_file_path, header=excel_header_row)

    prices_data = prices_data[prices_data['Plant'] == plant].reset_index(drop=True)

    prices_data.rename(columns=columns_to_rename, inplace=True)

    prices_data.to_excel(output_file_path, index=False)

clean_prices_table(
    'data/prices.xlsx',
    'data/clean_prices_basic.xlsx',
    excel_header_row=2,
)