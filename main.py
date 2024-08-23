from extract import extract
import pandas as pd

api_key = "4JKLD6C4SJYP77PN"
top_companies = ['AAPL']
file_path = f"C:\\Users\\ratho\\PycharmProjects\\stock_market\\stock_data\\combined_df.csv"

combined_df = pd.DataFrame()

for symbol in top_companies:
    df = extract(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}")
    combined_df = pd.concat([combined_df, df], axis=0)

print(combined_df)
combined_df.to_csv(file_path, index=False)