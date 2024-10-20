from extract import extract
from load_to_redshift import load_to_redshift
from load_to_s3 import upload_file_to_s3
import pandas as pd

api_key = "4JKLD6C4SJYP77PN"
top_companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'BRK.A', 'NVDA', 'JPM', 'V']
file_path = f"C:\\Users\\ratho\\PycharmProjects\\stock_market\\stock_data\\combined_df.csv"
bucket_name = "stocks-market-data"

combined_df = pd.DataFrame()

for symbol in top_companies:
    df = extract(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}")
    combined_df = pd.concat([combined_df, df], axis=0)

combined_df.to_csv(file_path, index=False)

upload_file_to_s3(file_path, bucket_name)

print("File Uploaded to S3 Succesfully")

load_to_redshift()

print("Data Successfully Loaded To Redshift")