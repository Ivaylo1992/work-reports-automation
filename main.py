import pandas as pd

from prices.prices import clean_prices_table
from stock.stock import move_columns, process_stock_report_df, create_pivot_table_df, merge_tables
from utils.column_creators import create_column
from utils.formulas import markup, percentage

# data_frame = pd.read_excel("data/stock.xlsx", header=2)
# new_data_frame = process_stock_report_df(data_frame)
# new_data_frame.to_excel("data/first_edit_stock.xlsx", index=False)

# data_frame = pd.read_excel("data/first_edit_stock.xlsx")
# new_data_frame = create_pivot_table_df(data_frame)
# new_data_frame.to_excel("data/second_edit_stock.xlsx")

# stock_data = pd.read_excel("data/second_edit_stock.xlsx")
# prices_data = pd.read_excel("data/prices.xlsx", header=2)
#
# cleaned_prices = clean_prices_table(prices_data)
#
# merged_df = merge_tables(cleaned_prices, stock_data)
# merged_df.to_excel("data/third_edit_stock.xlsx")

# data_frame = pd.read_excel("data/third_edit_stock.xlsx")
# new_data = create_column(data_frame, 'Markup', formula=markup)
# new_data_moved = move_columns(data_frame, 'Subgen', columns_to_move=['SalePrice', 'InitialPrice', 'PurchasePrice', 'Markup'])
# new_data_moved.to_excel("data/fifth_edit_stock.xlsx")


data_frame = pd.read_excel("data/fifth_edit_stock.xlsx")

data_frame = create_column(data_frame, '%', 'Markup', percentage)

data_frame.to_excel("data/sixth_edit_stock.xlsx", index=False)