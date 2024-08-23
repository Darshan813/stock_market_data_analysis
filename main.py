from extract import extract
import pandas as pd

api_key = "E4WGQCYBC9QSRC8O"
top_companies = ['AAPL', "IBM"]
file_path = f"C:\\Users\\ratho\\PycharmProjects\\stock_market\\stock_data\\combined_df.csv"

combined_df = pd.DataFrame()

for symbol in top_companies:
    df = extract(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}")
    combined_ = pd.concat([combined_df, df], axis=0)

print(combined_df)
combined_df.to_csv(file_path, index=False)

