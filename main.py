import pandas as pd

def process_stock_report(
        input_file_path: str,
        output_file_path: str,
        concept_filter: str = "OUTLET",
        columns_to_drop: list = None,
        columns_to_format_as_int: list = None,
        exel_header_row: int = 2,
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
                                          Defaults to ['AVAILABLE', 'RESERVED'].
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

        df_processed = (
          pd.read_excel(input_file_path, header=exel_header_row)
          .drop(columns=columns_to_drop)
          .query(f'Concept == "{concept_filter}"')
          .reset_index(drop=True)
          .assign(**format_assignments)
        )

        df_processed.to_excel(output_file_path, index=False)

        print(f"Saved to {output_file_path}")

process_stock_report(
    input_file_path='data/Available Stock Report.xlsx',
    output_file_path='data/new_report_basic.xlsx'
)
